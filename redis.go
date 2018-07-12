package mredis

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"math/rand"
	"time"
)

type Option struct {
	DbId      int
	MaxIdle   int
	MaxActive int
	Address   string
}

type RWPool struct {
	wPool    *redis.Pool
	rPools   []*redis.Pool
}

func NewRedigoPool(addr string, maxIdle, maxActive, db int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: 120 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr, redis.DialDatabase(db))
			if err != nil {
				return nil, err
			}
			return c, err
		},

		// NOTE Comment for preference
		/*TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		*/

		Wait: true,
	}
}

//
func NewPool(address string, maxIdle, maxActive, db int) *RWPool {
	wpool := NewRedigoPool(address, maxIdle, maxActive, db)
	rp := &RWPool{wPool: wpool}
	rp.getReadConnection = rp.getWriteConnection
	return rp
}

//
func NewRWPool(woption Option, roptions []Option) *RWPool {
	rpools := make([]*redis.Pool, len(roptions))
	for idx, roption := range roptions {
		rpools[idx] = NewRedigoPool(roption.Address, roption.MaxIdle, roption.MaxActive, roption.DbId)
	}

	wpool := NewRedigoPool(woption.Address, woption.MaxIdle, woption.MaxActive, woption.DbId)

	rp := &RWPool{wpool,rpools}
	go rp.checkSlave()

	return rp
}

//仅检查从库是否健康, 剔除掉线从库
func (rp *RWPool) checkSlave() {
	rnum := len(rp.rPools)
	rpool := make([]*redis.Pool, rnum)
	copy(rpool, rp.rPools)

	tmp := make([]*redis.Pool, rnum)
	for {
		cnt := 0
		time.Sleep(3 * time.Second)
		for idx, rp := range rpool {
			conn := rp.Get()
			_, err := conn.Do("PING")
			if err != nil {
				fmt.Printf("Kick Slave %d [%v]\n", idx, err)
			} else {
				tmp[cnt] = rp
				cnt++
			}
			conn.Close()
		}

		if cnt > 0 {  // slice alive slave
			rp.rPools = tmp[:cnt]
		}
	}
}

func (rp *RWPool) GetStat() (res map[string]int) {
	res = make(map[string]int)
	res["master"] = rp.wPool.ActiveCount()
	for i, p := range rp.rPools {
		res[fmt.Sprintf("slave_%d", i)] = p.ActiveCount()
	}

	return
}

func (rp *RWPool) getReadConnection(db int) redis.Conn {
	rpool := rp.rPools
	rp_size := len(rpool)
	if rp_size == 0 {
		return rp.getWriteConnection(db)
	}

	return rpool[rand.Int()%rp_size].Get()
}

//BDPool[db] = newPool(rp.allRPools[selected].Address, rp.maxActiveConn, db)
func (rp *RWPool) getWriteConnection(db int) redis.Conn {
	return rp.wPool.Get()
}


//
//type errorConnection struct{ err error }
//
//func (ec errorConnection) Do(string, ...interface{}) (interface{}, error) { return nil, ec.err }
//func (ec errorConnection) Send(string, ...interface{}) error              { return ec.err }
//func (ec errorConnection) Err() error                                     { return ec.err }
//func (ec errorConnection) Close() error                                   { return ec.err }
//func (ec errorConnection) Flush() error                                   { return ec.err }
//func (ec errorConnection) Receive() (interface{}, error)                  { return nil, ec.err }
