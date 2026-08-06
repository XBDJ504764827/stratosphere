/*
	Stratosphere - Backfill
	地图加载时扫描 data/gokz-replays/_runs/<map>/，把已有服务器纪录补齐上传到 R2。
	幂等：依据本地状态缓存（cache.sp）跳过已上传且未变化的纪录。
*/
