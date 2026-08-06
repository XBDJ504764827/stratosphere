/*
	Stratosphere - Helpers
	工具函数：_runs 文件名解析、录像 header 成绩读取（v1/v2）、
	键名/路径构建、文件复制、字符串工具。
*/



// =====[ CONSTANTS ]=====

#define ST_REPLAY_DIRECTORY "data/gokz-replays/_runs"   // GOKZ 永久录像目录（相对 Path_SM）
#define ST_DATA_DIRECTORY "data/gokz-stratosphere"      // 插件数据目录（缓存 + 暂存）
#define ST_STAGING_DIRECTORY "data/gokz-stratosphere/staging" // 上传暂存目录
#define ST_REPLAY_MAGIC 0x676F6B7A                      // 'gokz' 录像魔数
#define ST_MAX_KEY_LENGTH 128                           // 缓存键最大长度

int gI_StageCounter; // 暂存文件名自增，避免同键连传时互相覆盖（跨 include 使用，不能 static）



// =====[ STRINGS ]=====

// 原地转小写
void ST_ToLower(char[] buffer, int maxlength)
{
	for (int i = 0; buffer[i] != '\0' && i < maxlength; i++)
	{
		if (buffer[i] >= 'A' && buffer[i] <= 'Z')
		{
			buffer[i] = view_as<char>(buffer[i] + 32);
		}
	}
}

// 取路径中的文件名（去掉目录部分）
void ST_GetFileName(const char[] path, char[] output, int maxlength)
{
	int last = -1;
	for (int i = 0; path[i] != '\0'; i++)
	{
		if (path[i] == '/' || path[i] == '\\')
		{
			last = i;
		}
	}
	if (last == -1)
	{
		strcopy(output, maxlength, path);
	}
	else
	{
		strcopy(output, maxlength, path[last + 1]);
	}
}

// 把非法文件名字符替换为 '_'（缓存键/暂存文件名用）
void ST_SanitizeForFile(const char[] input, char[] output, int maxlength)
{
	int outPos = 0;
	for (int i = 0; input[i] != '\0' && outPos < maxlength - 1; i++)
	{
		char c = input[i];
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
			|| (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-')
		{
			output[outPos++] = c;
		}
		else
		{
			output[outPos++] = '_';
		}
	}
	output[outPos] = '\0';
}

// 构建缓存键：<模式>_<地图>_<类型>，例如 skz_kz_bhop_easy_pro
void ST_BuildCacheKey(const char[] modeShort, const char[] map, const char[] typeStr, char[] output, int maxlength)
{
	char safeMap[64];
	ST_SanitizeForFile(map, safeMap, sizeof(safeMap));
	Format(output, maxlength, "%s_%s_%s", modeShort, safeMap, typeStr);
}



// =====[ REPLAY FILE NAME ]=====

// 解析 GOKZ 永久录像文件名：<course>_<MODE>_<STYLE>_<TIMETYPE>.replay
// 例：0_SKZ_NRM_PRO.replay -> course=0, modeShort="skz", typeStr="pro"
// 只认 course 0、合法模式（vnl/skz/kzt）；NUB 及未知时间类型一律按 tp。
bool ST_ParseRunFileName(const char[] fileName, int &course, char[] modeShort, int modeShortLen, char[] typeStr, int typeStrLen)
{
	char buf[PLATFORM_MAX_PATH];
	strcopy(buf, sizeof(buf), fileName);

	// 去掉扩展名
	int dot = StrContains(buf, ".replay");
	if (dot == -1)
	{
		return false;
	}
	buf[dot] = '\0';

	// 按 '_' 切 4 段
	char parts[4][16];
	int n = ExplodeString(buf, "_", parts, sizeof(parts), sizeof(parts[]));
	if (n < 4)
	{
		return false;
	}

	course = StringToInt(parts[0]);
	if (course != 0)
	{
		return false; // 只处理主图
	}

	strcopy(modeShort, modeShortLen, parts[1]);
	ST_ToLower(modeShort, modeShortLen);
	if (!StrEqual(modeShort, "vnl") && !StrEqual(modeShort, "skz") && !StrEqual(modeShort, "kzt"))
	{
		return false;
	}

	if (StrEqual(parts[3], "PRO", false))
	{
		strcopy(typeStr, typeStrLen, "pro");
	}
	else
	{
		strcopy(typeStr, typeStrLen, "tp");
	}
	return true;
}



// =====[ REPLAY HEADER TIME ]=====

// 从录像文件读取成绩（秒）。支持 v2（当前 GOKZ 格式）与 v1（旧版本）。
bool ST_ReadReplayTime(const char[] path, float &time)
{
	File file = OpenFile(path, "rb");
	if (file == null)
	{
		return false;
	}

	int magic;
	if (!file.ReadInt32(magic) || magic != ST_REPLAY_MAGIC)
	{
		delete file;
		return false;
	}

	int version;
	if (!file.ReadInt8(version))
	{
		delete file;
		return false;
	}

	bool ok;
	if (version == 2)
	{
		ok = ST_ReadV2Time(file, time);
	}
	else if (version == 1)
	{
		ok = ST_ReadV1Time(file, time);
	}
	else
	{
		ok = false;
	}

	delete file;
	return ok;
}

// v2：GeneralHeader + RunHeader（time = int32 float 位模式）
static bool ST_ReadV2Time(File file, float &time)
{
	int dummy;

	int replayType;
	if (!file.ReadInt8(replayType) || replayType != 0)
	{
		return false; // 非 Run 录像
	}
	if (!ST_SkipString(file) // gokzVersion
		|| !ST_SkipString(file)) // mapName
	{
		return false;
	}
	file.ReadInt32(dummy); // mapFileSize
	file.ReadInt32(dummy); // serverIP
	file.ReadInt32(dummy); // timestamp
	if (!ST_SkipString(file)) // playerAlias
	{
		return false;
	}
	file.ReadInt32(dummy); // playerSteamID
	file.ReadInt8(dummy);  // mode
	file.ReadInt8(dummy);  // style
	file.ReadInt32(dummy); // playerSensitivity
	file.ReadInt32(dummy); // playerMYaw
	file.ReadInt32(dummy); // tickrate
	file.ReadInt32(dummy); // tickCount
	file.ReadInt32(dummy); // equippedWeapon
	file.ReadInt32(dummy); // equippedKnife

	int timeAsInt;
	if (!file.ReadInt32(timeAsInt))
	{
		return false;
	}
	time = view_as<float>(timeAsInt);
	return time > 0.0;
}

// v1：魔数+版本 之后为 gokzVersion / mapName 字符串，
// course / mode / style / time(int32 float) / teleportsUsed / steamAccountID(int32) ...
static bool ST_ReadV1Time(File file, float &time)
{
	int dummy;

	if (!ST_SkipString(file) // gokzVersion
		|| !ST_SkipString(file)) // mapName
	{
		return false;
	}
	file.ReadInt32(dummy); // course
	file.ReadInt32(dummy); // mode
	file.ReadInt32(dummy); // style

	int timeAsInt;
	if (!file.ReadInt32(timeAsInt))
	{
		return false;
	}
	time = view_as<float>(timeAsInt);
	return time > 0.0;
}

// 跳过长度前缀字符串（int8 长度 + 字节）
static bool ST_SkipString(File file)
{
	int len;
	if (!file.ReadInt8(len))
	{
		return false;
	}
	if (len <= 0)
	{
		return len == 0; // 空字符串合法
	}
	int[] dummy = new int[len];
	return file.Read(dummy, len, 1) == len;
}



// =====[ FILE ]=====

// 二进制文件复制（shavit 风格，复制自 gokz-replays/nav.sp 的 File_Copy）
bool ST_FileCopy(const char[] source, const char[] destination)
{
	File fileSource = OpenFile(source, "rb");
	if (fileSource == null)
	{
		return false;
	}
	File fileDest = OpenFile(destination, "wb");
	if (fileDest == null)
	{
		delete fileSource;
		return false;
	}
	int[] buffer = new int[64];
	int count;
	while (!fileSource.EndOfFile())
	{
		count = fileSource.Read(buffer, 64, 1); // 每次 64 字节
		if (count <= 0)
		{
			break;
		}
		fileDest.Write(buffer, count, 1);
	}
	delete fileSource;
	delete fileDest;
	return true;
}
