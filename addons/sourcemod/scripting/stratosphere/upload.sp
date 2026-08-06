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
	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-Map", map);
	SteamWorks_SetHTTPRequestHeaderValue(hRequest, "X-Route", typeStr);

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
		LogMessage("[stratosphere] Uploading -> wr/%s/%s/%s.replay (timeMs=%d)", gokzMode, map, typeStr, timeMs);
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
