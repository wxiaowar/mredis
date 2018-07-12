package mredis

import redigo "github.com/gomodule/redigo/redis"

/*
	封装 GEO 的相关功能
*/

/*
	添加几个
*/
func (rp *RWPool) GeoAdd(db int, key interface{}, values ...interface{}) (e error) {
	args := make([]interface{}, len(values)+1)
	args[0] = key
	copy(args[1:], values)
	scon := rp.getWriteConnection(db)

	defer scon.Close()
	_, e = scon.Do("GEOADD", args...)
	return
}

/*
	计算两个成员的距离
	m for meters.
	km for kilometers.
	mi for miles.
	ft for feet.
*/
func (rp *RWPool) GeoDist(db int, key interface{}, mem1, mem2 uint32, unit string) (distance float64, e error) {
	scon := rp.getReadConnection(db)
	defer scon.Close()
	return redigo.Float64(scon.Do("GEODIST", key, mem1, mem2, unit))
}

/*func GeoHash()*/

func (rp *RWPool) GeoPos(mem1 ...uint32) error {
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
