# Stratosphere

CS:GO GOKZ 录像上传插件：把服务器上**每张地图的最快录像（服务器纪录 WR）**上传到 Cloudflare R2，供 [wayfinder](https://github.com/wqq/wayfinder) 寻路插件下载并绘制路线。

与 wayfinder 是一对：**stratosphere 负责上传（生产者）**，**wayfinder 负责下载（消费者）**。

## 特性

- 📤 新纪录自动上传：`GOKZ_RP_OnReplaySaved`（tempReplay=false，服务器纪录）→ 异步上传 R2
- 🗂️ 回填：地图加载时扫描 `data/gokz-replays/_runs/<map>/` 补齐 R2 已有纪录（幂等）
- 🔑 键名与 wayfinder 完全一致：`wr/{mode}/{map}/{tp|pro}{course}.replay`（course = GOKZ 课程号）
- ⏱️ 附带成绩元数据：`x-time-ms` / `x-timestamp`，Worker 写入 customMetadata 供 `?meta=1` 查询
- 🧱 模块化代码结构（单入口 .sp + 模块目录，最终只编译一个 stratosphere.smx）

## 目录结构

```
addons/sourcemod/scripting/stratosphere.sp   # 唯一入口
addons/sourcemod/scripting/stratosphere/     # 模块目录
addons/sourcemod/translations/               # 翻译文件
cfg/sourcemod/gokz/                          # 运行时自动生成配置
docs/DEVELOPMENT.md                          # 开发文档
```

## 编译

```sh
./build.sh    # 需要 SourceMod 1.11 spcomp，详见 docs/DEVELOPMENT.md
```

## 安装

1. 将 `addons/`、`cfg/` 合并进 CS:GO 服务器根目录
2. 在 `cfg/sourcemod/gokz/gokz-stratosphere.cfg` 中填入 `gokz_stratosphere_url` 与 `gokz_stratosphere_key`
3. 重启服务器或 `sm plugins load stratosphere`

## 依赖

- SourceMod 1.11
- SteamWorks 扩展
- GOKZ + gokz-replays 插件
- Cloudflare Worker（R2 中转，见 docs/DEVELOPMENT.md）
