package redis

import (
	"errors"
	"github.com/gomodule/redigo/redis"
)

// string 类型 添加, v 可以是任意类型
func (c Cache) StringSet(name string, v interface{}) error {
	conn := c.pool.Get()
	s, _ := Serialization(v) // 序列化
	defer conn.Close()
	_, err := conn.Do("SET", name, s)
	return err
}

// 获取 字符串类型的值
func (c Cache) StringGet(name string, v interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	if conn.Err() != nil {
		return conn.Err()
	}
	temp, err := redis.Bytes(conn.Do("Get", name))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return nil
		}
		return err
	}
	err = Deserialization(temp, &v) // 反序列化
	return err
}

func (c Cache) stringGetTest() {
	//var need []string
	//var need int32
	var need int64
	//Deserialization(aa, &need)
	c.StringGet("yang", &need)
}

// 判断所在的 key 是否存在
func (c Cache) Exist(name string) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Bool(conn.Do("EXISTS", name))
	return v, err
}

// 自增
func (c Cache) StringIncr(name string) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Int(conn.Do("INCR", name))
	return v, err
}

// 设置过期时间 （单位 秒）
func (c Cache) Expire(name string, newSecondsLifeTime int64) error {
	// 设置key 的过期时间
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("EXPIRE", name, newSecondsLifeTime)
	return err
}

// 删除指定的键
func (c Cache) Delete(keys ...interface{}) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Bool(conn.Do("DEL", keys...))
	return v, err
}

// 查看指定的长度
func (c Cache) StrLen(name string) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Int(conn.Do("STRLEN", name))
	return v, err
}

// //////////////////  hash ///////////
// 删除指定的 hash 键
func (c Cache) Hdel(name, key string) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	var err error
	v, err := redis.Bool(conn.Do("HDEL", name, key))
	return v, err
}

// 查看hash 中指定是否存在
func (c Cache) HExists(name, field string) (bool, error) {
	conn := c.pool.Get()
	defer conn.Close()
	var err error
	v, err := redis.Bool(conn.Do("HEXISTS", name, field))
	return v, err
}

// 获取hash 的键的个数
func (c Cache) HLen(name string) (int, error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Int(conn.Do("HLEN", name))
	return v, err
}

// 传入的 字段列表获得对应的值
func (c Cache) HMget(name string, fields ...string) ([]interface{}, error) {
	conn := c.pool.Get()
	defer conn.Close()
	args := []interface{}{name}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := redis.Values(conn.Do("HMGET", args...))

	return value, err
}

// 设置单个值, value 还可以是一个 map slice 等
func (c Cache) HSet(name string, key string, value interface{}) (err error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, _ := Serialization(value)
	_, err = conn.Do("HSET", name, key, v)
	return
}

// 设置多个值 , obj 可以是指针 slice map struct
func (c Cache) HMSet(name string, obj interface{}) (err error) {
	conn := c.pool.Get()
	defer conn.Close()
	_, err = conn.Do("HSET", redis.Args{}.Add(name).AddFlat(&obj)...)
	return
}

// 获取单个hash 中的值
func (c Cache) HGet(name, field string, v interface{}) (err error) {
	conn := c.pool.Get()
	defer conn.Close()
	temp, _ := redis.Bytes(conn.Do("Get", name, field))
	err = Deserialization(temp, &v) // 反序列化
	return
}

// set 集合

// 获取 set 集合中所有的元素, 想要什么类型的自己指定
func (c Cache) Smembers(name string, v interface{}) (err error) {
	conn := c.pool.Get()
	defer conn.Close()
	temp, _ := redis.Bytes(conn.Do("smembers", name))
	err = Deserialization(temp, &v)
	return err
}

// 获取集合中元素的个数
func (c Cache) ScardInt64s(name string) (int64, error) {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Int64(conn.Do("SCARD", name))
	return v, err
}
