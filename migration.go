package consistent_hash

import (
	"context"
	"errors"
	"math"
)

// 用户需要注册好闭包函数进来，核心是执行数据迁移操作的
type Migrator func(ctx context.Context, dataKeys map[string]struct{}, from, to string) error

func (c *ConsistentHash) migrateIn(ctx context.Context, virtualScore int32, nodeID string) (from, to string, datas map[string]struct{}, _err error) {
	// 使用方没有注入迁移函数，则直接返回
	if c.migrator == nil {
		return
	}

	// 首先根据 virtualScore，查看对应的节点列表，理论上可能存在多个节点共用一个 virtualScore 的情况
	nodes, err := c.hashRing.Node(ctx, virtualScore)
	if err != nil {
		_err = err
		return
	}

	// 倘若节点数量大于 1，则说明当前节点不是 virtualScore 的第一个节点，则无需进行迁移
	if len(nodes) > 1 {
		return
	}

	// 寻找上一个 virtualScore
	lastScore, err := c.hashRing.Floor(ctx, c.decrScore(virtualScore))
	if err != nil {
		_err = err
		return
	}

	// 如果没有其他的 virtualScore，说明整个 hash ring 只有当前节点这一个节点，则无需迁移
	if lastScore == -1 || lastScore == virtualScore {
		return
	}

	// 寻找下一个 virtualScore
	nextScore, err := c.hashRing.Ceiling(ctx, c.incrScore(virtualScore))
	if err != nil {
		_err = err
		return
	}

	// 如果没有其他的 virtualScore，说明整个 hash ring 只有当前节点这一个节点，则无需迁移
	if nextScore == -1 || nextScore == virtualScore {
		return
	}

	// patternOne: last-0-cur-next
	patternOne := lastScore > virtualScore
	// patternTwo: last-cur-0-next
	patternTwo := nextScore < virtualScore
	if patternOne {
		lastScore -= math.MaxInt32
	}

	if patternTwo {
		virtualScore -= math.MaxInt32
		lastScore -= math.MaxInt32
	}

	// 获取到 nextScore 对应的节点，需要从中获取到所有数据对应的 key
	nextNodes, err := c.hashRing.Node(ctx, nextScore)
	if err != nil {
		_err = err
		return
	}

	// 找到其中的首个节点，获取到数据 key
	if len(nextNodes) == 0 {
		return
	}

	dataKeys, err := c.hashRing.DataKeys(ctx, c.getNodeID(nextNodes[0]))
	if err != nil {
		_err = err
		return
	}

	datas = make(map[string]struct{})
	// 遍历数据 key
	for dataKey := range dataKeys {
		dataVirtualScore := c.encryptor.Encrypt(dataKey)
		if patternOne && dataVirtualScore > (lastScore+math.MaxInt32) {
			dataVirtualScore -= math.MaxInt32
		}

		if patternTwo {
			dataVirtualScore -= math.MaxInt32
		}

		if dataVirtualScore <= lastScore || dataVirtualScore > virtualScore {
			continue
		}

		// 需要迁移的数据
		datas[dataKey] = struct{}{}
	}

	if err = c.hashRing.DeleteNodeToDataKeys(ctx, c.getNodeID(nextNodes[0]), datas); err != nil {
		return "", "", nil, err
	}

	if err = c.hashRing.AddNodeToDataKeys(ctx, nodeID, datas); err != nil {
		return "", "", nil, err
	}

	// from to datas
	return c.getNodeID(nextNodes[0]), nodeID, datas, nil
}

func (c *ConsistentHash) migrateOut(ctx context.Context, virtualScore int32, nodeID string) (from, to string, datas map[string]struct{}, err error) {
	// 使用方没有注入迁移函数，则直接返回
	if c.migrator == nil {
		return
	}

	defer func() {
		if err != nil {
			return
		}
		if to == "" || len(datas) == 0 {
			return
		}

		if err = c.hashRing.DeleteNodeToDataKeys(ctx, nodeID, datas); err != nil {
			return
		}

		err = c.hashRing.AddNodeToDataKeys(ctx, to, datas)
	}()

	from = nodeID

	nodes, _err := c.hashRing.Node(ctx, virtualScore)
	if _err != nil {
		err = _err
		return
	}

	if len(nodes) == 0 {
		return
	}

	// 不是 virtualScore 下的首个节点，则无需迁移
	if c.getNodeID(nodes[0]) != nodeID {
		return
	}

	// 如果没有数据，则直接返回
	var allDatas map[string]struct{}
	if allDatas, err = c.hashRing.DataKeys(ctx, nodeID); err != nil {
		return
	}

	if len(allDatas) == 0 {
		return
	}

	// 遍历数据 key，找出其中属于 (lastScore, virtualScore] 范围的数据
	lastScore, _err := c.hashRing.Floor(ctx, c.decrScore(virtualScore))
	if _err != nil {
		err = _err
		return
	}

	var onlyScore bool
	if lastScore == -1 || lastScore == virtualScore {
		if len(nodes) == 1 {
			err = errors.New("no other no")
			return
		}
		onlyScore = true
	}

	pattern := lastScore > virtualScore
	if pattern {
		lastScore -= math.MaxInt32
	}

	datas = make(map[string]struct{})
	for data := range allDatas {
		if onlyScore {
			datas[data] = struct{}{}
			continue
		}
		dataScore := c.encryptor.Encrypt(data)
		if pattern && dataScore > lastScore+math.MaxInt32 {
			dataScore -= math.MaxInt32
		}
		if dataScore <= lastScore || dataScore > virtualScore {
			continue
		}
		datas[data] = struct{}{}
	}

	// 如果同一个 virtualScore 下存在多个节点，则直接委托给下一个节点
	if len(nodes) > 1 {
		to = c.getNodeID(nodes[1])
		return
	}

	// 寻找后继节点
	if to, err = c.getValidNextNode(ctx, virtualScore, nodeID, nil); err != nil {
		err = _err
		return
	}

	if to == "" {
		err = errors.New("no other node")
	}

	return
}

func (c *ConsistentHash) getValidNextNode(ctx context.Context, score int32, nodeID string, ranged map[int32]struct{}) (string, error) {
	// 寻找后继节点
	nextScore, err := c.hashRing.Ceiling(ctx, c.incrScore(score))
	if err != nil {
		return "", err
	}
	if nextScore == -1 {
		return "", nil
	}

	if _, ok := ranged[nextScore]; ok {
		return "", nil
	}

	// 后继节点的 key 必须不与自己相同，否则继续往下寻找
	nextNodes, err := c.hashRing.Node(ctx, nextScore)
	if err != nil {
		return "", err
	}

	if len(nextNodes) == 0 {
		return "", errors.New("next node empty")
	}

	if nextNode := c.getNodeID(nextNodes[0]); nextNode != nodeID {
		return nextNode, nil
	}

	if len(nextNodes) > 1 {
		return c.getNodeID(nextNodes[1]), nil
	}

	if ranged == nil {
		ranged = make(map[int32]struct{})
	}
	ranged[score] = struct{}{}

	// 寻找下一个目标
	return c.getValidNextNode(ctx, nextScore, nodeID, ranged)
}

func (c *ConsistentHash) incrScore(score int32) int32 {
	if score == math.MaxInt32-1 {
		return 0
	}
	return score + 1
}

func (c *ConsistentHash) decrScore(score int32) int32 {
	if score == 0 {
		return math.MaxInt32 - 1
	}
	return score - 1
}
