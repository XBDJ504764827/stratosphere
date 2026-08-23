/*
	Stratosphere - Cache
	本地上传状态缓存：每个 R2 键一个标记文件（data/gokz-stratosphere/<键>.time），
	记录"上次成功与 Worker 交互时本服该纪录的成绩 time_ms"。

	语义：
	  - Worker 保证 R2 内容不慢于标记值（最快者胜）。
	  - 本地录像只会越破越快，因此回填时"缓存 time 与本地纪录一致"即可安全跳过。
	  - 上传失败不写缓存 → 下次地图循环的回填自动重试。

	R2 清空恢复：删除服务器上的 data/gokz-stratosphere/ 目录后，
	下次地图开始会全量重传（见 README）。
*/



// =====[ CACHE MARKERS ]=====

void ST_CacheRead(const char[] cacheKey, char[] output, int maxlength)
{
	output[0] = '\0';
	char path[PLATFORM_MAX_PATH];
	BuildPath(Path_SM, path, sizeof(path), "%s/%s.time", ST_DATA_DIRECTORY, cacheKey);
	if (!FileExists(path))
	{
		return;
	}
	File file = OpenFile(path, "r");
	if (file == null)
	{
		return;
	}
	file.ReadLine(output, maxlength);
	delete file;
	TrimString(output);
}

void ST_CacheWrite(const char[] cacheKey, int timeMs)
{
	ST_EnsureDataDir();
	char path[PLATFORM_MAX_PATH];
	BuildPath(Path_SM, path, sizeof(path), "%s/%s.time", ST_DATA_DIRECTORY, cacheKey);
	File file = OpenFile(path, "w");
	if (file == null)
	{
		LogError("[stratosphere] Failed to write cache marker: %s", path);
		return;
	}
	file.WriteLine("%d", timeMs);
	delete file;
}



// =====[ STAGING ]=====

// 同步复制录像到暂存目录，返回暂存路径；失败返回 false
bool ST_StageFile(const char[] sourcePath, const char[] cacheKey, char[] stagingPath, int maxlength)
{
	ST_EnsureStagingDir();

	gI_StageCounter++;
	char safeKey[ST_MAX_KEY_LENGTH];
	ST_SanitizeForFile(cacheKey, safeKey, sizeof(safeKey));
	BuildPath(Path_SM, stagingPath, maxlength, "%s/%s_%d.replay", ST_STAGING_DIRECTORY, safeKey, gI_StageCounter);
	return ST_FileCopy(sourcePath, stagingPath);
}

// 清理暂存目录（地图加载时调用；在途上传若因此失败，会由回填自动重试）
void ST_ClearStaging()
{
	char dir[PLATFORM_MAX_PATH];
	BuildPath(Path_SM, dir, sizeof(dir), "%s", ST_STAGING_DIRECTORY);
	if (!DirExists(dir))
	{
		return;
	}
	DirectoryListing listing = OpenDirectory(dir);
	if (listing == null)
	{
		return;
	}
	char fileName[PLATFORM_MAX_PATH];
	char fullPath[PLATFORM_MAX_PATH];
	FileType type;
	while (listing.GetNext(fileName, sizeof(fileName), type))
	{
		if (type != FileType_File)
		{
			continue;
		}
		BuildPath(Path_SM, fullPath, sizeof(fullPath), "%s/%s", ST_STAGING_DIRECTORY, fileName);
		DeleteFile(fullPath);
	}
	delete listing;
}



// =====[ PRIVATE ]=====

static void ST_EnsureDataDir()
{
	char dir[PLATFORM_MAX_PATH];
	BuildPath(Path_SM, dir, sizeof(dir), "%s", ST_DATA_DIRECTORY);
	if (!DirExists(dir))
	{
		CreateDirectory(dir, 511);
	}
}

// CreateDirectory 不创建中间目录，必须逐级创建
static void ST_EnsureStagingDir()
{
	char dir[PLATFORM_MAX_PATH];
	BuildPath(Path_SM, dir, sizeof(dir), "%s", ST_DATA_DIRECTORY);
	if (!DirExists(dir))
	{
		CreateDirectory(dir, 511);
	}
	BuildPath(Path_SM, dir, sizeof(dir), "%s", ST_STAGING_DIRECTORY);
	if (!DirExists(dir))
	{
		CreateDirectory(dir, 511);
	}
}
