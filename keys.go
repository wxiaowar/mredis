package mredis

import (
	redigo "github.com/gomodule/redigo/redis"
)

func (rp *RWPool) Exists(db int, key interface{}) (bool, error) {
	conn := rp.getReadConnection(db)
	defer conn.Close()
	return redigo.Bool(conn.Do("EXISTS", key))
}

func (rp *RWPool) Del(db int, key interface{}) error {
	conn := rp.getWriteConnection(db)
	defer conn.Close()
	_, e := conn.Do("DEL", key)
	return e
}

/*
设置key的有效时间
*/
func (rp *RWPool) Expire(db, expire int, key interface{}) (e error) {
	conn := rp.getWriteConnection(db)
	defer conn.Close()
	_, e = conn.Do("EXPIRE", key, expire)
	return

}

/*
设置key的到期时间

	db: 数据库表ID
	expireat:缓存失效的到期时间(unix 时间戳)
	key: 键值
*/
func (rp *RWPool) Expireat(db int, expireat int64, key interface{}) (ret int, e error) {
	conn := rp.getWriteConnection(db)
	defer conn.Close()
	ret, e = redigo.Int(conn.Do("EXPIREAT", key, expireat))
	return

}

/*
MultiExpire 批量设置key的有效时间

	db: 数据库表ID
	expire:缓存失效时间(秒值)
	args:key的列表
*/
func (rp *RWPool) MultiExpire(db, expire int, args ...interface{}) (e error) {
	if len(args) <= 0 {
		return
	}
	fcon := rp.getWriteConnection(db)
	defer fcon.Close()
	if e := fcon.Send("MULTI"); e != nil {
		return e
	}
	for _, key := range args {
		if e := fcon.Send("EXPIRE", key, expire); e != nil {
			fcon.Send("DISCARD")
			return e
		}
	}
	if _, e := fcon.Do("EXEC"); e != nil {
		fcon.Send("DISCARD")
		return e
	}
	return nil
}

func (rp *RWPool) MultiExec(db int, cmd func(con redigo.Conn) error) error {
	fcon := rp.getWriteConnection(db)
	defer fcon.Close()
	if e := fcon.Send("MULTI"); e != nil {
		return e
	}
	if e := cmd(fcon); e != nil {
		fcon.Send("DISCARD")
		return e
	}
	if _, e := fcon.Do("EXEC"); e != nil {
		return e
	}
	return nil
}

type ScanResult struct {
	Cursor string
	Keys   []string
}

func (rp *RWPool) KeyScan(db int, cursor string) (r ScanResult, e error) {
	fcon := rp.getWriteConnection(db)
	defer fcon.Close()
	reply, e := redigo.Values(fcon.Do("SCAN", cursor, "COUNT", 1000))
	if e != nil {
		return
	}
	if len(reply) == 2 {
		r.Cursor, e = redigo.String(reply[0], nil)
		r.Keys, e = redigo.Strings(reply[1], nil)
	}
	// fmt.Println(fmt.Sprintf("reply %v", reply))
	// e = ScanStruct(reply, &r)
	return
}

func (rp *RWPool) KeyScanWithPattern(db int, cursor string, pattern string) (r ScanResult, e error) {
	fcon := rp.getWriteConnection(db)
	defer fcon.Close()
	reply, e := redigo.Values(fcon.Do("SCAN", cursor, "MATCH", pattern, "COUNT", 100))
	if e != nil {
		return
	}
	if len(reply) == 2 {
		r.Cursor, e = redigo.String(reply[0], nil)
		r.Keys, e = redigo.Strings(reply[1], nil)
	}
	return
}

/*
获取key的有效时间
*/
func (rp *RWPool) TTL(db int, key interface{}) (expire int, e error) {
	conn := rp.getWriteConnection(db)
	defer conn.Close()
	expire, e = redigo.Int(conn.Do("TTL", key))
	return

}

func (rp *RWPool) Keys(db int, pattern string) (keys []string, e error) {
	conn := rp.getReadConnection(db)
	defer conn.Close()
	keys, e = redigo.Strings(conn.Do("KEYS", pattern))
	return
}
