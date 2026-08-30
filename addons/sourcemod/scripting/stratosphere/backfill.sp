/*
	Stratosphere - Backfill
	地图加载 3 秒后扫描 data/gokz-replays/_runs/<map>/，把 course 0 的
	服务器纪录补齐上传到 R2。
	幂等：本地缓存标记与录像 header 成绩一致则跳过（见 cache.sp）。
*/



public Action ST_Timer_BackfillMap(Handle timer)
{
	ST_BackfillMap(gC_Map);
	return Plugin_Stop;
}

void ST_BackfillMap(const char[] map)
{
	if (!gB_SteamWorksOK || map[0] == '\0' || !gCV_Enabled.BoolValue)
	{
		return;
	}

	char dir[PLATFORM_MAX_PATH];
	BuildPath(Path_SM, dir, sizeof(dir), "%s/%s", ST_REPLAY_DIRECTORY, map);

	DirectoryListing listing = OpenDirectory(dir);
	if (listing == null)
	{
		return; // 本图还没有任何录像
	}

	char fileName[PLATFORM_MAX_PATH];
	char fullPath[PLATFORM_MAX_PATH];
	char stagingPath[PLATFORM_MAX_PATH];
	FileType fileType;

	while (listing.GetNext(fileName, sizeof(fileName), fileType))
	{
		if (fileType != FileType_File)
		{
			continue;
		}

		char steamId[32];
		int course;
		char modeShort[8];
		char style[16];
		char typeStr[8];
		if (!ST_ParseRunFileNameFull(fileName, steamId, sizeof(steamId), course, modeShort, sizeof(modeShort), style, sizeof(style), typeStr, sizeof(typeStr)))
		{
			continue;
		}

		BuildPath(Path_SM, fullPath, sizeof(fullPath), "%s/%s", dir, fileName);

		float replayTime;
		if (!ST_ReadReplayTime(fullPath, replayTime))
		{
			LogError("[stratosphere] Failed to read replay time (backfill): %s", fullPath);
			continue;
		}
		int timeMs = RoundToNearest(replayTime * 1000.0);

		char cacheKey[ST_MAX_KEY_LENGTH];
		ST_BuildCacheKeyEx("0", course, modeShort, style, typeStr, map, cacheKey, sizeof(cacheKey));

		char cached[32];
		ST_CacheRead(cacheKey, cached, sizeof(cached));
		char timeStr[16];
		IntToString(timeMs, timeStr, sizeof(timeStr));
		if (StrEqual(cached, timeStr))
		{
			continue;
		}

		if (!ST_StageFile(fullPath, cacheKey, stagingPath, sizeof(stagingPath)))
		{
			LogError("[stratosphere] Failed to stage replay (backfill): %s", fullPath);
			continue;
		}

		if (gCV_Debug.BoolValue)
		{
			char modeUpper[8], styleUpper[16], typeUpper[8];
			strcopy(modeUpper, sizeof(modeUpper), modeShort); ST_ToUpper(modeUpper, sizeof(modeUpper));
			strcopy(typeUpper, sizeof(typeUpper), typeStr); ST_ToUpper(typeUpper, sizeof(typeUpper));
			strcopy(styleUpper, sizeof(styleUpper), style); ST_ToUpper(styleUpper, sizeof(styleUpper));
			LogMessage("[stratosphere] Backfill -> wr/%s/0_%d_%s_%s_%s.replay (timeMs=%d)", map, course, modeUpper, styleUpper, typeUpper, timeMs);
		}

		ST_UploadFileEx(stagingPath, "0", course, modeShort, style, map, typeStr, timeMs, cacheKey);
	}
	delete listing;
}
