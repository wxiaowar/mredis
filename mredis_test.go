package mredis

import (
	"testing"
	"go/src/fmt"
)

var p *RedisPool

func init() {
	p = NewPool("127.0.0.1:6379", "", 2, 2, 0)
}

//
func TestNewPool(t *testing.T) {
	mk := "hkey"
	for i:=0; i<5; i++ {
		key := fmt.Sprintf("key_%d", i)
		//val := fmt.Sprintf("val_%d", i)
		p.HSet(0, mk, key, i)
	}


	reply, err := p.HGetAll(0, mk)
	if err != nil {
		fmt.Printf("hgetall %v\n", err)
		return
	}

	fmt.Sprintf("reply = %v\n", reply)
	for idx := range reply {
		bts := reply[idx].([]byte)
		fmt.Printf("idx[%d] %s = %v\n", idx, string(bts), reply[idx])
	}

	r, err := p.HIncrBy(0, mk, "key_1", 1000000)
	fmt.Printf("hincrby %v-%v\n", r, err)


	r2, err := p.HGet(0, mk, "key_1")
	fmt.Printf("hget %v-%v\n", r2, err)
}

//
func TestRwPool(t *testing.T) {
	return
	wop := Option{
		DbId: 0,
		Address:"127.0.0.1:6379",
		MaxIdle: 2,
		MaxActive: 2,
	}

	rop := []Option{
		{
			DbId: 0,
			Address:"127.0.0.1:6380",
			MaxIdle: 2,
			MaxActive: 2,
		},
	}

	p := NewRWPool(wop, rop)

	mk := "hkey2"
	for i:=10; i<15; i++ {
		key := fmt.Sprintf("key_%d", i)
		//val := fmt.Sprintf("val_%d", i)
		p.HSet(0, mk, key, i)
		fmt.Printf("hset %s->%d\n", key, i)
	}


	reply, err := p.HGetAll(0, mk)
	if err != nil {
		fmt.Printf("hgetall %v\n", err)
		return
	}

	fmt.Sprintf("reply = %v\n", reply)
	for idx := range reply {
		bts := reply[idx].([]byte)
		fmt.Printf("idx[%d] %s = %v\n", idx, string(bts), reply[idx])
	}
}
