package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/demdxx/gocast"
	"github.com/gomodule/redigo/redis"
)

var ErrScoreNotExist = errors.New("score not exist")

// Client Redis 客户端.
type Client struct {
	opts *ClientOptions
	pool *redis.Pool
}

func NewClient(network, address, password string, opts ...ClientOption) *Client {
	c := Client{
		opts: &ClientOptions{
			network:  network,
			address:  address,
			password: password,
		},
	}

	for _, opt := range opts {
		opt(c.opts)
	}

	repairClient(c.opts)

	pool := c.getRedisPool()
	return &Client{
		pool: pool,
	}
}

func (c *Client) getRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     c.opts.maxIdle,
		IdleTimeout: time.Duration(c.opts.idleTimeoutSeconds) * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := c.getRedisConn()
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		MaxActive: c.opts.maxActive,
		Wait:      c.opts.wait,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func (c *Client) GetConn(ctx context.Context) (redis.Conn, error) {
	return c.pool.GetContext(ctx)
}

func (c *Client) getRedisConn() (redis.Conn, error) {
	if c.opts.address == "" {
		panic("Cannot get redis address from config")
	}

	var dialOpts []redis.DialOption
	if len(c.opts.password) > 0 {
		dialOpts = append(dialOpts, redis.DialPassword(c.opts.password))
	}
	conn, err := redis.DialContext(context.Background(),
		c.opts.network, c.opts.address, dialOpts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// ZAdd 执行Redis ZAdd 命令.
func (c *Client) ZAdd(ctx context.Context, table string, score int64, value string) error {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("ZADD", table, score, value)
	return err
}

type ScoreEntity struct {
	Score int64
	Val   string
}

// ZRangeByScore 执行 redis zrangebyscore 命令
func (c *Client) ZRangeByScore(ctx context.Context, table string, score1, score2 int64) ([]*ScoreEntity, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	raws, err := redis.Values(conn.Do("ZRANGE", table, score1, score2, "BYSCORE", "WITHSCORES"))
	if err != nil {
		return nil, err
	}

	if len(raws)&1 != 0 {
		return nil, fmt.Errorf("invalid entity len: %d", len(raws))
	}

	scoreEntities := make([]*ScoreEntity, 0, len(raws)>>1)
	for i := 0; i < len(raws)>>1; i++ {
		scoreEntities = append(scoreEntities, &ScoreEntity{
			Score: gocast.ToInt64(raws[i<<1|1]),
			Val:   gocast.ToString(raws[i<<1]),
		})
	}

	return scoreEntities, nil
}

// 返回大于等于 score 的第一个目标
func (c *Client) Ceiling(ctx context.Context, table string, score int64) (*ScoreEntity, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	raws, err := redis.Values(conn.Do("ZRANGE", table, score, "+inf", "BYSCORE", "LIMIT", 0, 1, "WITHSCORES"))
	if err != nil {
		return nil, err
	}

	if len(raws) != 2 {
		return nil, fmt.Errorf("invalid len of entity: %d, err: %w", len(raws), ErrScoreNotExist)
	}

	return &ScoreEntity{
		Score: gocast.ToInt64(raws[1]),
		Val:   gocast.ToString(raws[0]),
	}, nil
}

// 返回小于等于 score 的第一个目标
func (c *Client) Floor(ctx context.Context, table string, score int64) (*ScoreEntity, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	raws, err := redis.Values(conn.Do("ZRANGE", table, score, "-inf", "REV", "BYSCORE", "LIMIT", 0, 1, "WITHSCORES"))
	if err != nil {
		return nil, err
	}

	if len(raws) != 2 {
		return nil, fmt.Errorf("invalid len of entity: %d, err: %w", len(raws), ErrScoreNotExist)
	}

	return &ScoreEntity{
		Score: gocast.ToInt64(raws[1]),
		Val:   gocast.ToString(raws[0]),
	}, nil
}

func (c *Client) FirstOrLast(ctx context.Context, table string, first bool) (*ScoreEntity, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var raws []interface{}
	if first {
		raws, err = redis.Values(conn.Do("ZRANGE", table, "-inf", "+inf", "BYSCORE", "LIMIT", 0, 1, "WITHSCORES"))
	} else {
		raws, err = redis.Values(conn.Do("ZRANGE", table, "+inf", "-inf", "REV", "BYSCORE", "LIMIT", 0, 1, "WITHSCORES"))
	}

	if err != nil {
		return nil, err
	}

	if len(raws) != 2 {
		return nil, fmt.Errorf("invalid len of entity: %d, err: %w", len(raws), ErrScoreNotExist)
	}

	return &ScoreEntity{
		Score: gocast.ToInt64(raws[1]),
		Val:   gocast.ToString(raws[0]),
	}, nil
}

func (c *Client) ZRem(ctx context.Context, table string, score int64) error {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("ZREMRANGEBYSCORE", table, score, score)
	return err
}

func (c *Client) HSet(ctx context.Context, table, key, val string) error {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("HSET", table, key, val)
	return err
}

func (c *Client) HGetAll(ctx context.Context, table string) (map[string]string, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return redis.StringMap(conn.Do("HGETALL", table))
}

func (c *Client) HDel(ctx context.Context, table, key string) error {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("HDEL", table, key)
	return err
}

func (c *Client) Set(ctx context.Context, key, val string) error {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("SET", key, val)
	return err
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return redis.String(conn.Do("GET", key))
}

func (c *Client) Del(ctx context.Context, key string) error {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("DEL", key)
	return err
}

// Eval 支持使用 lua 脚本.
func (c *Client) Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error) {
	args := make([]interface{}, 2+len(keysAndArgs))
	args[0] = src
	args[1] = keyCount
	copy(args[2:], keysAndArgs)

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	return conn.Do("EVAL", args...)
}

func (c *Client) SetNEX(ctx context.Context, key, value string, expireSeconds int64) (int64, error) {
	if key == "" || value == "" {
		return -1, errors.New("redis SET keyNX or value can't be empty")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	reply, err := conn.Do("SET", key, value, "EX", expireSeconds, "NX")
	if err != nil {
		return -1, err
	}

	if respStr, ok := reply.(string); ok && strings.ToLower(respStr) == "ok" {
		return 1, nil
	}

	return redis.Int64(reply, err)
}
