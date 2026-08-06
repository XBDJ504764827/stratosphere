/*
	Stratosphere - ConVars
	ConVar 创建（autoexecconfig 自动生成 cfg/sourcemod/gokz/gokz-stratosphere.cfg，
	与其它 GOKZ 配置同目录）。
*/

void ST_CreateConVars()
{
	AutoExecConfig_SetFile("gokz-stratosphere", "sourcemod/gokz");
	AutoExecConfig_SetCreateFile(true);

	gCV_Enabled = AutoExecConfig_CreateConVar("gokz_stratosphere_enabled", "1", "总开关：是否上传录像到 R2。", _, true, 0.0, true, 1.0);
	gCV_URL     = AutoExecConfig_CreateConVar("gokz_stratosphere_url", "", "Cloudflare Worker 地址(根路径)，例如 https://cngokzreplay.iquankz.cn");
	gCV_Key     = AutoExecConfig_CreateConVar("gokz_stratosphere_key", "", "与 Worker 约定的 X-API-Key 密钥。");
	gCV_Debug   = AutoExecConfig_CreateConVar("gokz_stratosphere_debug", "0", "是否打印调试日志(验证通过后建议改回 0)。", _, true, 0.0, true, 1.0);

	AutoExecConfig_ExecuteFile();
	AutoExecConfig_CleanFile();
}
