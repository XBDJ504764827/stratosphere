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

// 原地转大写
void ST_ToUpper(char[] buffer, int maxlength)
{
	for (int i = 0; buffer[i] != '\0' && i < maxlength; i++)
	{
		if (buffer[i] >= 'a' && buffer[i] <= 'z')
		{
			buffer[i] = view_as<char>(buffer[i] - 32);
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

// 构建缓存键（新结构 5 段）：<steamId>_<course>_<模式>_<风格>_<类型>_<地图>
// 例: 0_0_kzt_nrm_pro_kz_bhop_easy  对应 R2 键 wr/kz_bhop_easy/0_0_KZT_NRM_PRO.replay
void ST_BuildCacheKeyEx(const char[] steamId, int course, const char[] modeShort, const char[] style, const char[] typeStr, const char[] map, char[] output, int maxlength)
{
	char safeMap[64];
	ST_SanitizeForFile(map, safeMap, sizeof(safeMap));
	char safeSteamId[32];
	ST_SanitizeForFile(steamId, safeSteamId, sizeof(safeSteamId));
	char safeMode[8], safeStyle[16], safeType[8];
	strcopy(safeMode, sizeof(safeMode), modeShort);
	ST_ToLower(safeMode, sizeof(safeMode));
	strcopy(safeStyle, sizeof(safeStyle), style);
	ST_ToLower(safeStyle, sizeof(safeStyle));
	strcopy(safeType, sizeof(safeType), typeStr);
	ST_ToLower(safeType, sizeof(safeType));
	Format(output, maxlength, "%s_%d_%s_%s_%s_%s", safeSteamId, course, safeMode, safeStyle, safeType, safeMap);
}

// 旧接口兼容：默认 steamId=0 course=0 style=NRM（WR 通用键）
#pragma unused ST_BuildCacheKey
void ST_BuildCacheKey(const char[] modeShort, const char[] map, const char[] typeStr, char[] output, int maxlength)
{
	ST_BuildCacheKeyEx("0", 0, modeShort, "NRM", typeStr, map, output, maxlength);
}



// =====[ REPLAY FILE NAME ]=====

// 解析 GOKZ 永久录像文件名
// 支持两种格式：
//  4 段: <course>_<MODE>_<STYLE>_<TIMETYPE>.replay  例: 0_SKZ_NRM_PRO.replay
//  5 段: <steamId>_<course>_<MODE>_<STYLE>_<TIMETYPE>.replay 例: 0_0_KZT_NRM_NUB.replay / 365313220_0_SKZ_NRM_NUB.replay
// 只认 course 0、合法模式（vnl/skz/kzt）；NUB 及未知时间类型一律按 tp。
#pragma unused ST_ParseRunFileName
bool ST_ParseRunFileName(const char[] fileName, int &course, char[] modeShort, int modeShortLen, char[] typeStr, int typeStrLen)
{
	char buf[PLATFORM_MAX_PATH];
	strcopy(buf, sizeof(buf), fileName);

	int dot = StrContains(buf, ".replay");
	if (dot == -1)
	{
		return false;
	}
	buf[dot] = '\0';

	char parts[5][16];
	int n = ExplodeString(buf, "_", parts, sizeof(parts), sizeof(parts[]));
	if (n == 5)
	{
		// 5 段含 steamId 前缀
		course = StringToInt(parts[1]);
		if (course != 0)
		{
			return false;
		}
		strcopy(modeShort, modeShortLen, parts[2]);
		ST_ToLower(modeShort, modeShortLen);
		if (!StrEqual(modeShort, "vnl") && !StrEqual(modeShort, "skz") && !StrEqual(modeShort, "kzt"))
		{
			return false;
		}
		if (StrEqual(parts[4], "PRO", false))
		{
			strcopy(typeStr, typeStrLen, "pro");
		}
		else
		{
			strcopy(typeStr, typeStrLen, "tp");
		}
		return true;
	}
	if (n < 4)
	{
		return false;
	}
	course = StringToInt(parts[0]);
	if (course != 0)
	{
		return false;
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

// 完整解析（5 段新结构）：返回 steamId/course/mode/style/type
bool ST_ParseRunFileNameFull(const char[] fileName, char[] steamId, int steamIdLen, int &course, char[] modeShort, int modeShortLen, char[] style, int styleLen, char[] typeStr, int typeStrLen)
{
	char buf[PLATFORM_MAX_PATH];
	strcopy(buf, sizeof(buf), fileName);
	int dot = StrContains(buf, ".replay");
	if (dot == -1)
	{
		return false;
	}
	buf[dot] = '\0';
	char parts[5][16];
	int n = ExplodeString(buf, "_", parts, sizeof(parts), sizeof(parts[]));
	if (n == 5)
	{
		strcopy(steamId, steamIdLen, parts[0]);
		course = StringToInt(parts[1]);
		if (course != 0) return false;
		strcopy(modeShort, modeShortLen, parts[2]);
		ST_ToLower(modeShort, modeShortLen);
		if (!StrEqual(modeShort, "vnl") && !StrEqual(modeShort, "skz") && !StrEqual(modeShort, "kzt")) return false;
		strcopy(style, styleLen, parts[3]);
		ST_ToLower(style, styleLen);
		if (StrEqual(parts[4], "PRO", false)) strcopy(typeStr, typeStrLen, "pro");
		else strcopy(typeStr, typeStrLen, "tp");
		return true;
	}
	if (n < 4) return false;
	strcopy(steamId, steamIdLen, "0");
	course = StringToInt(parts[0]);
	if (course != 0) return false;
	strcopy(modeShort, modeShortLen, parts[1]);
	ST_ToLower(modeShort, modeShortLen);
	if (!StrEqual(modeShort, "vnl") && !StrEqual(modeShort, "skz") && !StrEqual(modeShort, "kzt")) return false;
	strcopy(style, styleLen, parts[2]);
	ST_ToLower(style, styleLen);
	if (StrEqual(parts[3], "PRO", false)) strcopy(typeStr, typeStrLen, "pro");
	else strcopy(typeStr, typeStrLen, "tp");
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

// 解析录像源路径：兼容三种形式
//   1. 绝对路径（当前 gokz 用 BuildPath(Path_SM) 生成）
//   2. 相对游戏目录（旧版 gokz 以 "addons/sourcemod/data/..." 传入 forward）
//   3. 相对 SourceMod 目录（"data/gokz-replays/..."）
static bool ST_ResolveSourcePath(const char[] source, char[] output, int maxlength)
{
	// 绝对路径（Unix '/' 或 Windows 盘符）
	if (source[0] == '/' || (source[1] == ':' && ((source[0] >= 'A' && source[0] <= 'Z') || (source[0] >= 'a' && source[0] <= 'z'))))
	{
		strcopy(output, maxlength, source);
		return true;
	}

	// 以 "addons/..." 开头 = 相对游戏目录（csgo/），
	// 游戏目录 = Path_SM 去掉末尾的 /addons/sourcemod（SM 1.11 无 Path_Game）
	if (StrContains(source, "addons/") == 0)
	{
		char gameDir[PLATFORM_MAX_PATH];
		BuildPath(Path_SM, gameDir, sizeof(gameDir), "");
		int slashPos = StrContains(gameDir, "/addons/sourcemod");
		if (slashPos == -1)
		{
			slashPos = StrContains(gameDir, "\\addons\\sourcemod"); // Windows
		}
		if (slashPos != -1)
		{
			gameDir[slashPos] = '\0';
			Format(output, maxlength, "%s/%s", gameDir, source);
		}
		else
		{
			// 找不到则退回直接传原路径（OpenFile 默认按游戏目录解析）
			strcopy(output, maxlength, source);
		}
		return true;
	}

	// 其余按相对 SourceMod 目录解析
	BuildPath(Path_SM, output, maxlength, "%s", source);
	return true;
}

// 二进制文件复制（shavit 风格，复制自 gokz-replays/nav.sp 的 File_Copy）
bool ST_FileCopy(const char[] source, const char[] destination)
{
	char resolvedSource[PLATFORM_MAX_PATH];
	ST_ResolveSourcePath(source, resolvedSource, sizeof(resolvedSource));

	File fileSource = OpenFile(resolvedSource, "rb");
	if (fileSource == null)
	{
		LogError("[stratosphere] ST_FileCopy: cannot open source: %s", resolvedSource);
		return false;
	}
	File fileDest = OpenFile(destination, "wb");
	if (fileDest == null)
	{
		LogError("[stratosphere] ST_FileCopy: cannot open destination: %s", destination);
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
