package mredis

import redigo "github.com/gomodule/redigo/redis"

/*
	封装 GEO 的相关功能
*/

/*
	添加几个
*/
func (rp *RedisPool) GeoAdd(key string, values ...interface{}) (int64, error) {
	args := make([]interface{}, 0, len(values)+1)
	args = append(args, key)
	args = append(args, values...)

	//args[0] = key
	//copy(args[1:], values)
	con := rp.getConn()
	defer con.Close()

	return redigo.Int64(con.Do("GEOADD", args...))
}

/*
	计算两个成员的距离
	m for meters.
	km for kilometers.
	mi for miles.
	ft for feet.
*/
func (rp *RedisPool) GeoDist(key interface{}, mem1, mem2 uint32, unit string) (distance float64, e error) {
	con := rp.getConn()
	defer con.Close()

	return redigo.Float64(con.Do("GEODIST", key, mem1, mem2, unit))
}

/*func GeoHash()*/

func (rp *RedisPool) GeoPos(mem1 ...uint32) error {
	return nil
}

/*


GEOADD
GEODIST
GEOHASH
GEOPOS
GEORADIUS
GEORADIUSBYMEMBER

*/
