# Stratosphere 开发文档

> CS:GO GOKZ 录像上传插件 —— 把服务器纪录（WR）录像上传到 Cloudflare R2（Worker 中转），供 wayfinder 寻路插件下载。

- 插件名：`stratosphere`
- 最终产物：**一个** `stratosphere.smx`
- 代码结构：模仿 `~/gokz-top-plugins`（单入口 `.sp` + 同名模块目录）
- 生命周期管理：模仿 `~/gokz/gokz-core.sp`（入口文件集中接收 SourceMod/GOKZ 事件，转发给各模块）
- R2 交互：参考 `~/gokz-r2upload`（SteamWorks HTTP + Worker 中转鉴权），但键名对齐 wayfinder，且不再使用旧插件的"位移"（tp1/pro1）方案

---

## 1. 项目概述

### 1.1 解决什么问题

wayfinder 从 R2 按 `wr/{mode}/{map}/{tp|pro}.replay` 拉取录像绘制路线。
stratosphere 是配套的生产端：每当服务器上出现新的服务器纪录（WR），把该录像上传到 R2 的对应键，
并在地图加载时回填历史纪录，保证 R2 始终持有本服（及多服中最快）的录像。

### 1.2 核心约束

| 约束 | 说明 |
|---|---|
| 单一 SMX | 所有模块编译进一个 `stratosphere.smx` |
| 依赖最少 | 运行时：SourceMod 1.11、SteamWorks、gokz-core、gokz-replays |
| 异步上传 | 用 SteamWorks 回调，绝不阻塞主线程 |
| 不打扰 | 上传失败仅记日志；forward 永远 Plugin_Continue，不影响 GOKZ 本地保存 |
| 幂等回填 | 地图加载时的回填不重复上传（本地 time_ms 标记） |
| 无玩家界面 | 无命令、无菜单、无翻译文件 |

### 1.3 R2 键名约定（与 wayfinder 对齐）

```
wr/{mode}/{map}/{tp|pro}.replay
  mode = vnl | skz | kzt（GOKZ 模式，小写）
  map  = 服务器当前地图名（小写；与 gokz-replays 的 _runs 目录命名一致，
         即 GetCurrentMapDisplayName 的结果）
  tp   = 存点（有 TP）最快录像
  pro  = 不存点（无 TP）最快录像
```

- **只处理 course 0（主图）**：bonus 录像忽略（wayfinder 的主图请求会回退到无课程号格式）。
- `_runs` 文件（`<course>_<MODE>_<STYLE>_<TIMETYPE>.replay`，本 GOKZ 版本 STYLE_COUNT=1）与 R2 键一一对应。

### 1.4 Worker 协议（已部署，含"最快者胜"）

- 上传：`POST {url}`，headers：
  - `X-API-Key`（鉴权）、`X-GOKZ-Mode`（vnl/skz/kzt）、`X-Map`、`X-Route`（tp/pro）、`X-Time-Ms`（毫秒）、`X-Timestamp`（unix 秒）
  - body = .replay 二进制
- Worker 存到 `wr/{mode}/{map}/{route}.replay`，customMetadata 写入 time_ms/uploaded_at/sha256
- **最快者胜**：POST 时先 `head()` 现有对象，sha256 相同或新成绩不更快 → 跳过并返回 `stored:false`
- 查询：`GET {url}/{key}?meta=1` → `{exists, time_ms, uploaded_at, size, sha256}`（wayfinder 用它比对缓存）
- ⚠️ 旧 gokz-r2upload 发送 `X-Mode` 头，Worker 实际读取 `x-route`；stratosphere 按 Worker 源码发送 `X-Route`

---

## 2. 目录结构

```
stratosphere/                       # 项目根（= 本仓库）
├── README.md
├── docs/DEVELOPMENT.md             # 本文档
├── build.sh                        # 编译脚本（./build.sh setup 首次准备环境）
├── .github/workflows/build.yml     # CI：自动编译 + 验证单一 smx
├── cfg/sourcemod/gokz/
│   └── gokz-stratosphere.cfg       # ConVar 默认模板（运行时 autoexecconfig 自动补齐）
└── addons/sourcemod/
    ├── plugins/                    # 编译产物 stratosphere.smx
    └── scripting/
        ├── stratosphere.sp         # 唯一入口
        └── stratosphere/           # 模块目录（均被入口 include）
            ├── convars.sp          # ConVar 创建（autoexecconfig）
            ├── helpers.sp          # 文件名解析 / 录像 header 读时 / 键名与路径构建 / 文件复制
            ├── cache.sp            # 本地 time_ms 标记 + 上传暂存目录管理
            ├── upload.sp           # SteamWorks POST 原语 + 回调
            ├── events.sp           # 新纪录上传（forward 处理）
            └── backfill.sp         # 地图加载回填
```

include 顺序 = 依赖顺序：convars → helpers → cache → upload → events → backfill。

## 3. 模块说明

### 3.1 入口 stratosphere.sp

- `OnPluginStart`：创建 ConVar
- `OnAllPluginsLoaded`：检查 SteamWorks 扩展与 gokz-replays 库
- `OnMapStart`：记录当前地图名（小写）→ 清理暂存目录 → 3 秒后回填
- `GOKZ_RP_OnReplaySaved`：只认 `ReplayType_Run` + `tempReplay==false` + `course==0`，永远 `Plugin_Continue`

### 3.2 events.sp —— 新纪录上传

`ST_OnRecordSaved(map, time, filePath)`：从文件名解析模式/类型（不依赖玩家当前选项）→
同步复制到暂存（防覆盖竞态）→ `ST_UploadFile`。

### 3.3 backfill.sp —— 回填

`ST_BackfillMap(map)`：扫描 `data/gokz-replays/_runs/<map>/`，对每个 course 0 文件：
读 header 成绩 → 与缓存标记比对，一致跳过 → 否则暂存 + 上传。
**上传失败不写缓存** → 下次地图循环自动重试。

### 3.4 cache.sp —— 缓存与暂存

- 标记文件：`data/gokz-stratosphere/<模式>_<地图>_<类型>.time`，内容为 time_ms
- 语义：Worker 保证 R2 内容不慢于标记值；本地纪录只会越破越快 → 标记一致即可跳过
- 暂存：`data/gokz-stratosphere/staging/`，文件名带自增计数器避免同键连传冲突
- R2 清空恢复：删除 `data/gokz-stratosphere/` 目录 → 下次地图开始全量重传

### 3.5 upload.sp —— HTTP 上传

- 超时 60s；headers 按 Worker 协议；body 从暂存文件流式读取（不占堆内存）
- 回调（带 DataPack 上下文：timeMs/cacheKey/stagingPath）：
  - 2xx → `ST_CacheWrite` + 删除暂存副本
  - 其它 → 记错误日志 + 删除暂存副本（缓存不更新）

### 3.6 helpers.sp —— 工具

- `ST_ParseRunFileName`：`<course>_<MODE>_<STYLE>_<TIMETYPE>.replay` → course/modeShort/typeStr
- `ST_ReadReplayTime`：读录像 header 成绩，支持 v1/v2 格式（布局参照 wayfinder 的 replayfile.sp）
- `ST_BuildCacheKey` / `ST_SanitizeForFile` / `ST_ToLower` / `ST_GetFileName` / `ST_FileCopy`

## 4. ConVar

| ConVar | 默认 | 说明 |
|---|---|---|
| `gokz_stratosphere_enabled` | 1 | 总开关 |
| `gokz_stratosphere_url` | （空） | Worker 地址（根路径） |
| `gokz_stratosphere_key` | （空） | X-API-Key |
| `gokz_stratosphere_debug` | 0 | 调试日志 |

配置文件生成在 `cfg/sourcemod/gokz/gokz-stratosphere.cfg`（与其它 GOKZ 配置同目录）。

## 5. 编译与部署

```sh
./build.sh setup   # 首次：下载 SourceMod 1.11 + GOKZ include 依赖
./build.sh         # 编译 → addons/sourcemod/plugins/stratosphere.smx
```

部署：合并 `addons/`、`cfg/` 到服务器根目录；填写 URL/KEY；重启或 `sm plugins load stratosphere`。
多开部署：**每台游戏服都要装**；共用一个 Worker 与 bucket（最快者胜保证 R2 内容为全服最快）。
