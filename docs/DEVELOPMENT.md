# Stratosphere 开发文档

> CS:GO GOKZ 录像上传插件 —— 把服务器纪录（WR）录像上传到 Cloudflare R2（Worker 中转），供 wayfinder 寻路插件下载。

- 插件名：`stratosphere`
- 最终产物：**一个** `stratosphere.smx`
- 代码结构：模仿 `~/gokz-top-plugins`（单入口 `.sp` + 同名模块目录）
- 生命周期管理：模仿 `~/gokz/gokz-core.sp`（入口文件集中接收 SourceMod/GOKZ 事件，转发给各模块）
- R2 交互：参考 `~/gokz-r2upload`（SteamWorks HTTP + Worker 中转鉴权），但键名对齐 wayfinder

---

## 1. 项目概述

### 1.1 解决什么问题

wayfinder 从 R2 按 `wr/{mode}/{map}/{tp|pro}{course}.replay` 拉取录像绘制路线。
stratosphere 是配套的生产端：每当服务器上出现新的服务器纪录（WR），把该录像上传到 R2 的对应键，
并在地图加载时回填历史纪录，保证 R2 始终持有本服最快录像。

### 1.2 核心约束

| 约束 | 说明 |
|---|---|
| 单一 SMX | 所有模块编译进一个 `stratosphere.smx` |
| 依赖最少 | 运行时：SourceMod 1.11、SteamWorks、gokz-core、gokz-replays |
| 异步上传 | 用 SteamWorks 回调，绝不阻塞主线程 |
| 不打扰 | 上传失败仅记日志，不影响 GOKZ 本地保存（forward 永远 Plugin_Continue） |
| 幂等回填 | 地图加载时的回填不重复上传（本地状态缓存） |

### 1.3 R2 键名约定（与 wayfinder 对齐）

```
wr/{mode}/{map}/{type}{course}.replay
  mode   = vnl | skz | kzt（GOKZ 模式，小写）
  map    = 服务器当前地图名（小写）
  type   = pro（无 TP）| tp（有 TP）
  course = GOKZ 课程号：0 = 主图，1/2/... = bonus
```

每个 `_runs` 文件（`<course>_<MODE>_NRM_<TIMETYPE>.replay`）一一对应一个 R2 键，无歧义。

### 1.4 Worker 协议

- 上传：`POST {url}`，headers：`X-API-Key`、`X-GOKZ-Mode`（vnl/skz/kzt）、`X-Map`、`X-Route`（tp0/pro0/...）、`X-Time-Ms`、`X-Timestamp`；body = .replay 二进制
- Worker 存到 `wr/{mode}/{map}/{route}.replay`，customMetadata 写入 time_ms/uploaded_at/sha256
- 注意：**旧 gokz-r2upload 发送 `X-Mode`，Worker 实际读取 `x-route`**，stratosphere 按 Worker 源码发送 `X-Route`

---

（待补充：模块说明、ConVar 列表、部署步骤）
