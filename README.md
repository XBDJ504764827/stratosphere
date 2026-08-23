# Stratosphere

CS:GO GOKZ 录像上传插件：把服务器上**每张地图的最快录像（服务器纪录 WR）**上传到 Cloudflare R2，供 [wayfinder](https://github.com/wqq/wayfinder) 寻路插件下载并绘制路线。

与 wayfinder 是一对：**stratosphere 负责上传（生产者）**，**wayfinder 负责下载（消费者）**。

## 特性

- 📤 新纪录自动上传：`GOKZ_RP_OnReplaySaved`（tempReplay=false，打破服务器纪录）→ 异步上传 R2
- 🗂️ 回填：地图加载时扫描 `data/gokz-replays/_runs/<map>/` 补齐 R2 已有纪录（幂等，本地 time_ms 标记比对）
- 🏆 多服最快者胜：共用 bucket 的多开服务器由 Worker 按成绩比对，**慢纪录不会覆盖快纪录**
- 🔑 键名与 wayfinder 完全一致：`wr/{mode}/{map}/{tp|pro}.replay`（tp = 存点最快，pro = 不存点最快）
- 🛡️ 竞态防护：上传前同步复制到暂存目录，异步上传期间新纪录覆盖源文件不影响上传
- ⏱️ 附带成绩元数据：`x-time-ms` / `x-timestamp`，Worker 写入 customMetadata 供 `?meta=1` 查询
- 🧱 模块化代码结构（单入口 .sp + 模块目录，最终只编译一个 stratosphere.smx）
- 🔇 纯后台插件：无任何命令、无玩家可见文本，仅日志

## 目录结构

```
addons/sourcemod/scripting/stratosphere.sp   # 唯一入口
addons/sourcemod/scripting/stratosphere/     # 模块目录
cfg/sourcemod/gokz/                          # 运行时自动生成配置
docs/DEVELOPMENT.md                          # 开发文档
```

## 编译

```sh
./build.sh    # 需要 SourceMod 1.11 spcomp，详见 docs/DEVELOPMENT.md
```

## 安装

1. 将 `addons/`、`cfg/` 合并进 CS:GO 服务器根目录
2. 在 `cfg/sourcemod/gokz/gokz-stratosphere.cfg` 中填入 `gokz_stratosphere_url`（Worker 地址）与 `gokz_stratosphere_key`（与 Worker 约定的密钥）
3. 重启服务器或 `sm plugins load stratosphere`

## R2 清空恢复

如果 R2 bucket 被清空/更换：在**每台**游戏服务器上删除 `addons/sourcemod/data/gokz-stratosphere/` 目录，下次地图开始会自动全量重传。

## 与 wayfinder 的配合

```
打破服务器纪录 → stratosphere 上传 wr/{mode}/{map}/{tp|pro}.replay
→ wayfinder 下载并解析轨迹 → 玩家看到路线指引
```

- 键名约定完全一致：`wr/{mode}/{map}/{tp|pro}.replay`（tp = 存点最快，pro = 不存点最快）
- 多开服务器共用 bucket 时，Worker 按成绩"最快者胜"，wayfinder 拿到的始终是全服最快录像
- 本插件不依赖 wayfinder 运行（R2 数据可被其他工具消费），但两者搭配才能形成"上传 → 指引"闭环

## 依赖

- SourceMod 1.11
- SteamWorks 扩展
- GOKZ + gokz-replays 插件
- Cloudflare Worker（R2 中转，支持"最快者胜"，见 docs/DEVELOPMENT.md §Worker 协议）
- **wayfinder（配套寻路插件）**：消费 R2 中的录像为玩家绘制路线（见上文"与 wayfinder 的配合"）；没有它时上传的录像没有展示端，但上传功能不受影响
