package consistent_hash

import (
	"context"
	"testing"
	"time"

	"github.com/xiaoxuxiansheng/consistent_hash/local"
	"github.com/xiaoxuxiansheng/consistent_hash/redis"
)

func Test_local_consistent_hash(t *testing.T) {
	localHashRing := local.NewSkiplistHashRing()
	murmurHasher := NewMurmurHasher()
	localMigrator := func(ctx context.Context, dataKeys map[string]struct{}, from, to string) error {
		t.Logf("from: %s, to: %s, data keys: %v", from, to, dataKeys)
		return nil
	}
	consistentHash := NewConsistentHash(
		localHashRing,
		murmurHasher,
		localMigrator,
		// 每个 node 对应的虚拟节点个数为权重 * replicas
		WithReplicas(5),
		// 加锁 5 s 后哈希环的锁自动释放
		WithLockExpireSeconds(5),
	)
	test(t, consistentHash)
}

const (
	network  = "tcp"
	address  = "redis 地址"
	password = "redis 密码"

	hashRingKey = "哈希环唯一 id"
)

func Test_redis_consistent_hash(t *testing.T) {
	redisClient := redis.NewClient(network, address, password)
	hashRing := redis.NewRedisHashRing(hashRingKey, redisClient)
	consistentHash := NewConsistentHash(hashRing, NewMurmurHasher(), nil)
	test(t, consistentHash)
}

func test(t *testing.T, consistentHash *ConsistentHash) {
	ctx := context.Background()
	nodeA := "node_a"
	weightNodeA := 2
	nodeB := "node_b"
	weightNodeB := 1
	nodeC := "node_c"
	weightNodeC := 1
	if err := consistentHash.AddNode(ctx, nodeA, weightNodeA); err != nil {
		t.Error(err)
		return
	}

	if err := consistentHash.AddNode(ctx, nodeB, weightNodeB); err != nil {
		t.Error(err)
		return
	}

	dataKeyA := "data_a"
	dataKeyB := "data_b"
	dataKeyC := "data_c"
	dataKeyD := "data_d"
	node, err := consistentHash.GetNode(ctx, dataKeyA)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyA, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyB); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyB, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyC); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyC, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyD); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyD, node)
	if err := consistentHash.AddNode(ctx, nodeC, weightNodeC); err != nil {
		t.Error(err)
		return
	}
	if node, err = consistentHash.GetNode(ctx, dataKeyA); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyA, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyB); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyB, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyC); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyC, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyD); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyD, node)
	if err = consistentHash.RemoveNode(ctx, nodeC); err != nil {
		t.Error(err)
		return
	}
	if node, err = consistentHash.GetNode(ctx, dataKeyA); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyA, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyB); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyB, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyC); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyC, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyD); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyD, node)
	t.Error("ok")
}

func Test_local_lock(t *testing.T) {
	hashRing := local.NewSkiplistHashRing()
	ctx := context.Background()
	if err := hashRing.Lock(ctx, 1); err != nil {
		t.Error(err)
		return
	}
	<-time.After(2 * time.Second)
	if err := hashRing.Lock(ctx, 2); err != nil {
		t.Error(err)
		return
	}
	if err := hashRing.Unlock(ctx); err != nil {
		t.Error(err)
		return
	}
}
