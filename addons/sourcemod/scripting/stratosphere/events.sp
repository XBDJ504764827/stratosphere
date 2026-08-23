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

	int course;
	char modeShort[8];
	char typeStr[8];
	if (!ST_ParseRunFileName(fileName, course, modeShort, sizeof(modeShort), typeStr, sizeof(typeStr)))
	{
		return;
	}

	char mapLower[64];
	strcopy(mapLower, sizeof(mapLower), map);
	ST_ToLower(mapLower, sizeof(mapLower));

	char cacheKey[ST_MAX_KEY_LENGTH];
	ST_BuildCacheKey(modeShort, mapLower, typeStr, cacheKey, sizeof(cacheKey));

	int timeMs = RoundToNearest(time * 1000.0);

	// 同步复制到暂存目录，避免异步上传期间同组合新纪录覆盖源文件
	char stagingPath[PLATFORM_MAX_PATH];
	if (!ST_StageFile(filePath, cacheKey, stagingPath, sizeof(stagingPath)))
	{
		LogError("[stratosphere] Failed to stage replay for upload: %s", filePath);
		return;
	}

	if (gCV_Debug.BoolValue)
	{
		LogMessage("[stratosphere] New record -> wr/%s/%s/%s.replay (timeMs=%d)", modeShort, mapLower, typeStr, timeMs);
	}

	ST_UploadFile(stagingPath, modeShort, mapLower, typeStr, timeMs, cacheKey);
}
