package mredis

// 生成redis分页
func buildRange(cur, ps int) (int, int) {
	begin := 0
	if cur > 1 {
		begin = (cur - 1) * ps
	}
	end := begin + ps - 1

	return begin, end
}
