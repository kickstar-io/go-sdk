package searedis

type RedisConnectionConfig struct {
	Addr             string
	UserName         string
	Password         string
	Database         int
	PoolSize         int
	SentinelPassword string
}
