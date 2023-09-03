package redis

import (
	"context"
	"testing"
)

const (
	network  = "tcp"
	address  = "请输入 redis 地址——{ip}:{port}"
	password = "请输入 redis 密码，若未设置密码则传空串"
)

func Test_ZRangeByScore(t *testing.T) {
	client := NewClient(network, address, password)
	ctx := context.Background()
	scoreEntity, err := client.ZRangeByScore(ctx, "my_zset", 1, 1)
	if err != nil {
		t.Error(err)
		return
	}
	t.Errorf("score: %v", scoreEntity)
}

func Test_Ceiling(t *testing.T) {
	client := NewClient(network, address, password)
	ctx := context.Background()
	scoreEntity, err := client.Ceiling(ctx, "my_zset", 3)
	if err != nil {
		t.Error(err)
		return
	}
	t.Errorf("scoreEntity: %+v", scoreEntity)
}

func Test_Floor(t *testing.T) {
	client := NewClient(network, address, password)
	ctx := context.Background()
	scoreEntity, err := client.Floor(ctx, "my_zset", 0)
	if err != nil {
		t.Error(err)
		return
	}
	t.Errorf("scoreEntity: %+v", scoreEntity)
}

func Test_Last(t *testing.T) {
	client := NewClient(network, address, password)
	ctx := context.Background()
	scoreEntity, err := client.FirstOrLast(ctx, "my_zset", false)
	if err != nil {
		t.Error(err)
		return
	}
	t.Errorf("scoreEntity: %+v", scoreEntity)
}

func Test_HGetAll(t *testing.T) {
	client := NewClient(network, address, password)
	ctx := context.Background()
	res, err := client.HGetAll(ctx, "my_hset")
	if err != nil {
		t.Error(err)
		return
	}
	t.Error(res)
}
