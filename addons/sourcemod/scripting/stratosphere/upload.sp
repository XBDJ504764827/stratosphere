/*
	Stratosphere - Upload
	SteamWorks HTTP POST 上传原语：把暂存录像上传到 Worker。
	成功（2xx）→ 更新本地缓存并删除暂存副本；失败 → 仅记日志，
	缓存不更新，下次地图循环的回填自动重试。
*/



// =====[ CONSTANTS ]=====

#define ST_HTTP_TIMEOUT 60
#define ST_MAX_URL_LENGTH 256
#define ST_MAX_KEY_LENGTH 128



// =====[ PUBLIC ]=====

void ST_UploadFile(const char[] stagingPath, const char[] gokzMode, const char[] map, const char[] typeStr, int timeMs, const char[] cacheKey)
{
	ST_UploadFileEx(stagingPath, "0", 0, gokzMode, "NRM", map, typeStr, timeMs, cacheKey);
}

// 新结构 5 段上传：额外携带 steamId/course/style
void ST_UploadFileEx(const char[] stagingPath, const char[] steamId, int course, const char[] gokzMode, const char[] style, const char[] map, const char[] typeStr, int timeMs, const char[] cacheKey)
{
	char url[ST_MAX_URL_LENGTH];
	gCV_URL.GetString(url, sizeof(url));
	if (url[0] == '\0')
	{
		if (gCV_Debug.BoolValue)
		{
			LogMessage("[stratosphere] Skipping upload: gokz_stratosphere_url is not set.");
		}
		return;
	}

	char apiKey[ST_MAX_KEY_LENGTH];
	gCV_Key.GetString(apiKey, sizeof(apiKey));

	if (gCV_Debug.BoolValue && apiKey[0] == '\0')
	{
		LogMessage("[stratosphere] gokz_stratosphere_key is EMPTY; uploads will be rejected with 401.");
	}

	Handle hRequest = SteamWorks_CreateHTTPRequest(k_EHTTPMethodPOST, url);
	if (hRequest == null)
	{
		LogError("[stratosphere] Failed to create HTTP request for %s.", cacheKey);
		return;
	}

	SteamWorks_SetHTTPRequestNetworkActivityTimeout(hRequest, ST_HTTP_TIMEOUT);
	SteamWorks_SetHTTPRequestAbsoluteTimeoutMS(hRequest, ST_HTTP_TIMEOUT * 1000);
	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-API-Key", apiKey);
	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-GOKZ-Mode", gokzMode);
	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-Mode", gokzMode);
	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-Map", map);
	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-Route", typeStr);
	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-Type", typeStr);

	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-SteamId", steamId);
	char courseStr[8];
	IntToString(course, courseStr, sizeof(courseStr));
	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-Course", courseStr);
	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-Style", style);

	char timeMsStr[16];
	IntToString(timeMs, timeMsStr, sizeof(timeMsStr));
	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-Time-Ms", timeMsStr);

	char timestampStr[16];
	IntToString(GetTime(), timestampStr, sizeof(timestampStr));
	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-Timestamp", timestampStr);

	char contentType[] = "application/octet-stream";
	if (!SteamWorks_SetHTTPRequestRawPostBodyFromFile(hRequest, contentType, stagingPath))
	{
		LogError("[stratosphere] Failed to set POST body from staging file: %s", stagingPath);
		delete hRequest;
		return;
	}

	DataPack pack = new DataPack();
	pack.WriteCell(timeMs);
	pack.WriteString(cacheKey);
	pack.WriteString(stagingPath);
	SteamWorks_SetHTTPRequestContextValue(hRequest, pack);
	SteamWorks_SetHTTPCallbacks(hRequest, ST_OnUploadCompleted);

	if (gCV_Debug.BoolValue)
	{
		// 新结构日志：wr/{map}/{steamId}_{course}_{mode}_{style}_{type}.replay
		char modeUpper[8], typeUpper[8], styleUpper[16];
		strcopy(modeUpper, sizeof(modeUpper), gokzMode); ST_ToUpper(modeUpper, sizeof(modeUpper));
		strcopy(typeUpper, sizeof(typeUpper), typeStr); ST_ToUpper(typeUpper, sizeof(typeUpper));
		strcopy(styleUpper, sizeof(styleUpper), style); ST_ToUpper(styleUpper, sizeof(styleUpper));
		LogMessage("[stratosphere] Uploading -> wr/%s/%s_%d_%s_%s_%s.replay (timeMs=%d, apiKey=%s)",
			map, steamId, course, modeUpper, styleUpper, typeUpper, timeMs, apiKey[0] == '\0' ? "NOT_SET" : "set");
	}

	if (!SteamWorks_SendHTTPRequest(hRequest))
	{
		LogError("[stratosphere] Failed to send HTTP request for %s.", cacheKey);
		delete pack;
		delete hRequest;
	}
}

public void ST_OnUploadCompleted(Handle hRequest, bool bFailure, bool bRequestSuccessful, EHTTPStatusCode eStatusCode, any data1)
{
	DataPack pack = view_as<DataPack>(data1);
	if (pack == null)
	{
		delete hRequest;
		return;
	}

	pack.Reset();
	int timeMs = pack.ReadCell();
	char cacheKey[ST_MAX_KEY_LENGTH];
	pack.ReadString(cacheKey, sizeof(cacheKey));
	char stagingPath[PLATFORM_MAX_PATH];
	pack.ReadString(stagingPath, sizeof(stagingPath));
	delete pack;

	int code = view_as<int>(eStatusCode);
	if (bFailure || !bRequestSuccessful || code < 200 || code >= 300)
	{
		LogError("[stratosphere] Upload failed: key=%s failure=%d successful=%d status=%d",
			cacheKey, bFailure ? 1 : 0, bRequestSuccessful ? 1 : 0, code);
		if (code == 401)
		{
			LogError("[stratosphere]   -> 401: gokz_stratosphere_key 与 Worker 的 API_KEY 不一致，或 Cloudflare 端未配置 API_KEY 变量。");
		}
		else if (code == 400)
		{
			LogError("[stratosphere]   -> 400: 请求头缺失或非法（X-GOKZ-Mode/X-Map/X-Route），检查 Worker 协议。");
		}
	}
	else
	{
		// 2xx 即成功（stored=false 表示 Worker 已持有相同或更快的录像），更新缓存
		ST_CacheWrite(cacheKey, timeMs);
		if (gCV_Debug.BoolValue)
		{
			LogMessage("[stratosphere] Upload OK: %s (timeMs=%d, status=%d)", cacheKey, timeMs, code);
		}
	}

	// 无论成败都清理暂存副本
	if (stagingPath[0] != '\0' && FileExists(stagingPath))
	{
		DeleteFile(stagingPath);
	}
	delete hRequest;
}
