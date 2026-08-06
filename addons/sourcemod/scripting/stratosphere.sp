/*
	Stratosphere
	---------------------------------------------
	CS:GO GOKZ 录像上传插件：把服务器上每张地图的最快录像（服务器纪录 WR）
	上传到 Cloudflare R2（通过 Worker 中转），供 wayfinder 寻路插件下载绘制路线。

	【键名约定（与 wayfinder 完全对齐）】
	  wr/<vnl|skz|kzt>/<地图名>/<tp|pro>.replay
	  例如 wr/skz/kz_bhop_easy/pro.replay
	  - 只处理 course 0（主图）；bonus 录像忽略
	  - tp = 存点（有 TP）最快录像，pro = 不存点（无 TP）最快录像
	  - 每个 (模式, 地图, 类型) 只有一个键；多服务器共用 bucket 时由 Worker
	    按 time_ms 做"最快者胜"，慢纪录不会覆盖快纪录

	【上传触发】
	  GOKZ_RP_OnReplaySaved：tempReplay == false（打破服务器纪录被永久保存）时上传。
	  forward 永远返回 Plugin_Continue，不影响 gokz-replays 的本地保存。

	【回填】
	  地图加载 3 秒后扫描 data/gokz-replays/_runs/<map>/ 中 course 0 的录像，
	  把 R2 缺失/过期的纪录补齐上传。幂等依据本地状态缓存
	  （data/gokz-stratosphere/ 下每键一个 time_ms 标记，见 cache.sp）。

	【竞态防护】
	  gokz-replays 在 forward 触发前已把录像完整写入磁盘；永久纪录不会被清理，
	  但同组合的新纪录会覆盖旧文件。为避免异步上传读到半截文件，上传前
	  先把录像同步复制到暂存目录（data/gokz-stratosphere/staging/）再从副本上传。

	【Worker 协议】
	  POST {gokz_stratosphere_url}
	  Headers:
	      X-API-Key:    <gokz_stratosphere_key>
	      X-GOKZ-Mode:  vnl|skz|kzt
	      X-Map:        <地图名>
	      X-Route:      tp|pro
	      X-Time-Ms:    <成绩毫秒>
	      X-Timestamp:  <unix 秒>
	  Body: .replay 文件二进制
	  响应 JSON: { success, stored, reason, path, time_ms, sha256, size }
	  （Worker 已实现最快者胜：stored=false 表示 R2 已有相同或更快的录像）

	依赖：SteamWorks 扩展、gokz-replays 插件。

	ConVar（首次启动自动生成 cfg/sourcemod/gokz/gokz-stratosphere.cfg）：
	  gokz_stratosphere_enabled "1"  总开关
	  gokz_stratosphere_url     ""   Worker 地址（根路径）
	  gokz_stratosphere_key     ""   X-API-Key
	  gokz_stratosphere_debug   "0"  调试日志
*/

#include <sourcemod>

#include <gokz>
#include <gokz/core>
#include <gokz/replays>

#undef REQUIRE_EXTENSIONS
#include <SteamWorks>

#include <autoexecconfig>

#pragma newdecls required
#pragma semicolon 1

#define ST_VERSION "1.0.0"

public Plugin myinfo =
{
	name = "Stratosphere",
	author = "wqq",
	description = "Uploads GOKZ server record (WR) replays to Cloudflare R2 via a Worker",
	version = ST_VERSION,
	url = ""
};



// =====[ GLOBAL STATE ]=====

ConVar gCV_Enabled;
ConVar gCV_URL;
ConVar gCV_Key;
ConVar gCV_Debug;

bool gB_SteamWorksOK;
char gC_Map[64]; // 当前地图名（小写，与 gokz-replays 的 _runs 目录命名一致）

// 模块 include 顺序 = 依赖顺序：被依赖的模块在前
#include "stratosphere/convars.sp"
#include "stratosphere/helpers.sp"
#include "stratosphere/cache.sp"
#include "stratosphere/upload.sp"
#include "stratosphere/events.sp"
#include "stratosphere/backfill.sp"



// =====[ PLUGIN LIFECYCLE ]=====

public void OnPluginStart()
{
	ST_CreateConVars();
}

public void OnAllPluginsLoaded()
{
	if (!LibraryExists("gokz-replays"))
	{
		LogError("[stratosphere] gokz-replays not found; relies on its GOKZ_RP_OnReplaySaved forward.");
	}
	gB_SteamWorksOK = (GetExtensionFileStatus("SteamWorks.ext") > 0);
	if (!gB_SteamWorksOK)
	{
		LogError("[stratosphere] SteamWorks extension not loaded; uploads disabled.");
	}
}

public void OnMapStart()
{
	GetCurrentMapDisplayName(gC_Map, sizeof(gC_Map));
	ST_ToLower(gC_Map, sizeof(gC_Map));
	ST_ClearStaging(); // 清理上次会话遗留的暂存文件（可能覆盖上一次地图加载的少量在途上传，失败会由回填自动重试）
	// 稍延迟，避免和地图加载抢占 IO
	CreateTimer(3.0, ST_Timer_BackfillMap);
}



// =====[ FORWARD FROM gokz-replays ]=====

public Action GOKZ_RP_OnReplaySaved(int client, int replayType,
	const char[] map, int course, int timeType, float time,
	const char[] filePath, bool tempReplay)
{
	// 永远放行，不干扰 gokz-replays 的本地保存与临时文件清理
	if (!gCV_Enabled.BoolValue)
	{
		return Plugin_Continue;
	}
	if (replayType != ReplayType_Run)
	{
		return Plugin_Continue;
	}
	// tempReplay == false = 永久保存 = 打破服务器纪录（首条纪录同样成立）
	if (tempReplay)
	{
		return Plugin_Continue;
	}
	if (filePath[0] == '\0')
	{
		return Plugin_Continue;
	}
	if (course != 0)
	{
		return Plugin_Continue; // 只上传主图（course 0）
	}

	ST_OnRecordSaved(map, time, filePath);
	return Plugin_Continue;
}
