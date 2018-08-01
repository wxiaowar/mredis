package mredis

import redigo "github.com/gomodule/redigo/redis"

/*
	封装 GEO 的相关功能
*/

/*
	添加几个
*/
func (rp *mPool) GeoAdd(db int, key interface{}, values ...interface{}) (int64, error) {
	args := make([]interface{}, len(values)+1)
	args[0] = key
	copy(args[1:], values)
	scon := rp.getWrite(db)

	defer scon.Close()
	return redigo.Int64(scon.Do("GEOADD", args...))
}

/*
	计算两个成员的距离
	m for meters.
	km for kilometers.
	mi for miles.
	ft for feet.
*/
func (rp *mPool) GeoDist(db int, key interface{}, mem1, mem2 uint32, unit string) (distance float64, e error) {
	scon := rp.getRead(db)
	defer scon.Close()
	return redigo.Float64(scon.Do("GEODIST", key, mem1, mem2, unit))
}

/*func GeoHash()*/

func (rp *mPool) GeoPos(mem1 ...uint32) error {
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
