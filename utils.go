package mredis

import (
	"fmt"
	"time"
)

// 生成redis分页
func buildRange(cur, ps int) (int, int) {
	begin := 0
	if cur > 1 {
		begin = (cur - 1) * ps
	}
	end := begin + ps - 1

	return begin, end
}

//生成score，时间会以秒数的形式存储在64位整型的前(64-bits)位，tag会存储在后bits位。
func MakeZScore(tm time.Time, tag uint32, bits uint) int64 {
	score := (tm.Unix() << bits) + int64(tag)
	fmt.Printf("tm=%b,tag=%b,score=%b(%u)\n", tm.Unix(), tag, score, score)
	return score
}

//从score中提取标签的值
func GetTagFromScore(score int64, bits uint) uint32 {
	fmt.Printf("score=%b,tag=%b\n", score, score&((int64(1)<<bits)-1))
	return uint32(score & ((int64(1) << bits) - 1))
}

//从score中提取时间
func GetTimeFromScore(score int64, bits uint) time.Time {
	fmt.Printf("tm=%b\n", score>>bits)
	return time.Unix(score>>bits, 0)
}
