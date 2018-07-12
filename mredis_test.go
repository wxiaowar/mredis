package mredis

import (
	"testing"
	"go/src/fmt"
)

//
func TestNewPool(t *testing.T) {
	p := NewPool("127.0.0.1:6379", 2, 2, 0)

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
}

//
func TestRwPool(t *testing.T) {
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
