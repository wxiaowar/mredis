package mredis

import (
	"fmt"
	"testing"
)

var p *RedisPool

func init() {
	p, _ = NewRedisPool("redis://user:1234@127.0.0.1:16379/0")
}

// 单连接
func TestNewPool(t *testing.T) {
	mk := "hkey"
	for i:=0; i<5; i++ {
		key := fmt.Sprintf("key_%d", i)
		p.HSet(mk, key, i)
	}

	m, err := p.HGetAll(mk).StringMap()
	if err != nil {
		fmt.Printf("hgetall error %v", err)
	}

	fmt.Printf("reply = %v\n", m)

	r, err := p.HIncByWithReturn( mk, "key_1", 1000000).Int64()
	fmt.Printf("hincrby %v-%v\n", r, err)


	r2, err := p.HGet( mk, "key_1").Int64()
	fmt.Printf("hget %v-%v\n", r2, err)
}