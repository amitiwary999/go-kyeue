package gokyeue

func InitStorage(connectionUrl string, poolLimit int16, timeout int16) (*PostgresDbClient, error) {
	return newPostgresClient(connectionUrl, poolLimit, timeout)
}
