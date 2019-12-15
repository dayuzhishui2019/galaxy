package redis

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"time"
)

var (
	DEFAULT = time.Duration(0)  // 过期时间 不设置
	FOREVER = time.Duration(-1) // 过期时间不设置
)

type Cache struct {
	pool              *redis.Pool
	defaultExpiration time.Duration
}

// 返回cache 对象, 在多个工具之间建立一个 中间初始化的时候使用
func NewRedisCache(db int, host string, defaultExpiration time.Duration) *Cache {
	pool := &redis.Pool{
		MaxActive:   100,                              //  最大连接数，即最多的tcp连接数，一般建议往大的配置，但不要超过操作系统文件句柄个数（centos下可以ulimit -n查看）
		MaxIdle:     100,                              // 最大空闲连接数，即会有这么多个连接提前等待着，但过了超时时间也会关闭。
		IdleTimeout: time.Duration(100) * time.Second, // 空闲连接超时时间，但应该设置比redis服务器超时时间短。否则服务端超时了，客户端保持着连接也没用
		Wait:        true,                             // 当超过最大连接数 是报错还是等待， true 等待 false 报错
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", host, redis.DialDatabase(db))
			if err != nil {
				fmt.Println(err)
				return nil, err
			}
			return conn, nil
		},
	}
	return &Cache{pool: pool, defaultExpiration: defaultExpiration}
}

func (c Cache) Close() (err error) {
	if c.pool != nil {
		return c.pool.Close()
	}
	return nil
}

func (c Cache) Inited() bool {
	return c.pool != nil
}
