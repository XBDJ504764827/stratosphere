/*
	Stratosphere - Events
	GOKZ_RP_OnReplaySaved（tempReplay == false，打破服务器纪录）→ 暂存 + 上传。
	只处理 course 0；模式/类型从录像文件名解析（与 gokz-replays 命名一致，
	不依赖玩家当前选项，回填路径可复用同一解析）。
*/



void ST_OnRecordSaved(const char[] map, float time, const char[] filePath)
{
	if (!gB_SteamWorksOK)
	{
		return;
	}

	char fileName[PLATFORM_MAX_PATH];
	ST_GetFileName(filePath, fileName, sizeof(fileName));

	char steamId[32];
	int course;
	char modeShort[8];
	char style[16];
	char typeStr[8];
	if (!ST_ParseRunFileNameFull(fileName, steamId, sizeof(steamId), course, modeShort, sizeof(modeShort), style, sizeof(style), typeStr, sizeof(typeStr)))
	{
		return;
	}

	char mapLower[64];
	strcopy(mapLower, sizeof(mapLower), map);
	ST_ToLower(mapLower, sizeof(mapLower));

	// WR 统一 steamId=0（通用最快纪录），本地文件名中的 steamId 仅用于回填去重时保留
	char cacheKey[ST_MAX_KEY_LENGTH];
	ST_BuildCacheKeyEx("0", course, modeShort, style, typeStr, mapLower, cacheKey, sizeof(cacheKey));

	int timeMs = RoundToNearest(time * 1000.0);

	char stagingPath[PLATFORM_MAX_PATH];
	if (!ST_StageFile(filePath, cacheKey, stagingPath, sizeof(stagingPath)))
	{
		LogError("[stratosphere] Failed to stage replay for upload: %s", filePath);
		return;
	}

	if (gCV_Debug.BoolValue)
	{
		char modeUpper[8], styleUpper[16], typeUpper[8];
		strcopy(modeUpper, sizeof(modeUpper), modeShort); ST_ToUpper(modeUpper, sizeof(modeUpper));
		strcopy(typeUpper, sizeof(typeUpper), typeStr); ST_ToUpper(typeUpper, sizeof(typeUpper));
		strcopy(styleUpper, sizeof(styleUpper), style); ST_ToUpper(styleUpper, sizeof(styleUpper));
		LogMessage("[stratosphere] New record -> wr/%s/0_%d_%s_%s_%s.replay (timeMs=%d)", mapLower, course, modeUpper, styleUpper, typeUpper, timeMs);
	}

	ST_UploadFileEx(stagingPath, "0", course, modeShort, style, mapLower, typeStr, timeMs, cacheKey);
}
