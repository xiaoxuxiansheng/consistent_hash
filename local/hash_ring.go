package local

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xiaoxuxiansheng/consistent_hash/pkg/os"
	"github.com/xiaoxuxiansheng/redis_lock/utils"
)

type LockEntityV2 struct {
	locked   bool
	mutex    sync.Mutex
	expireAt time.Time
	owner    string
}

func NewLockEntityV2() *LockEntityV2 {
	return &LockEntityV2{}
}

func (l *LockEntityV2) Lock(ctx context.Context, expireSeconds int) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	now := time.Now()
	if !l.locked || l.expireAt.Before(now) {
		l.locked = true
		l.expireAt = now.Add(time.Duration(expireSeconds) * time.Second)
		l.owner = utils.GetProcessAndGoroutineIDStr()
		return nil
	}

	return errors.New("accquire by others")
}

func (l *LockEntityV2) Unlock(ctx context.Context) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if !l.locked || l.expireAt.Before(time.Now()) {
		return errors.New("not locked")
	}

	if l.owner != utils.GetProcessAndGoroutineIDStr() {
		return errors.New("not your lock")
	}

	l.locked = false
	return nil
}

// 基于本地跳表实现一个 hash_ring
type SkiplistHashRing struct {
	LockEntity
	root *virtualNode
	// 每个节点对应的虚拟节点个数
	nodeToReplicas map[string]int
	nodeToDataKey  map[string]map[string]struct{}
}

type LockEntity struct {
	lock sync.Mutex
	// 另一把锁，为了支持解锁的幂等操作
	doubleLock sync.Mutex
	cancel     context.CancelFunc
	owner      atomic.Value
}

func NewSkiplistHashRing() *SkiplistHashRing {
	return &SkiplistHashRing{
		root:           &virtualNode{},
		nodeToReplicas: make(map[string]int),
		nodeToDataKey:  make(map[string]map[string]struct{}),
	}
}

type virtualNode struct {
	score int32
	// 存储的 nodeID 列表
	nodeIDs []string
	nexts   []*virtualNode
}

// 锁住哈希环，支持配置过期时间. 达到过期时间后，会自动释放锁
func (s *SkiplistHashRing) Lock(ctx context.Context, expireSeconds int) error {
	// 只锁定指定的时长
	s.doubleLock.Lock()
	defer s.doubleLock.Unlock()

	s.lock.Lock()
	token := os.GetCurrentProcessAndGogroutineIDStr()
	s.owner.Store(token)
	if expireSeconds <= 0 {
		return nil
	}

	// 先加锁，指定时长后进行解锁. 需要保证指定时长后锁的使用方还是自己, 不能解了别人的锁
	cctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go func() {
		// 发生一次解锁操作后，该 goroutine 会被终止
		select {
		case <-cctx.Done():
			return
		case <-time.After(time.Duration(expireSeconds) * time.Second):
			s.unlock(ctx, token)
		}
	}()
	return nil
}

func (s *SkiplistHashRing) unlock(ctx context.Context, token string) error {
	// 解锁. 倘若已经解锁，则直接返回，避免 fatal
	s.doubleLock.Lock()
	defer s.doubleLock.Unlock()
	// 锁如果不属于自己，直接终止
	owner, _ := s.owner.Load().(string)
	if owner != token {
		return errors.New("not your lock")
	}
	s.owner.Store("")

	// 是属于自己的锁，则先终止对应的守护 goroutine
	if s.cancel != nil {
		s.cancel()
	}

	s.lock.Unlock()
	return nil
}

func (s *SkiplistHashRing) Unlock(ctx context.Context) error {
	token := os.GetCurrentProcessAndGogroutineIDStr()
	return s.unlock(ctx, token)
}

func (s *SkiplistHashRing) Add(ctx context.Context, score int32, nodeID string) error {
	targetNode, ok := s.get(score)
	if ok {
		for _, _nodeID := range targetNode.nodeIDs {
			if _nodeID == nodeID {
				return nil
			}
		}
		targetNode.nodeIDs = append(targetNode.nodeIDs, nodeID)
		return nil
	}

	rLevel := s.roll()
	if len(s.root.nexts) < rLevel+1 {
		difs := make([]*virtualNode, rLevel+1-len(s.root.nexts))
		s.root.nexts = append(s.root.nexts, difs...)
	}

	newNode := virtualNode{
		score:   score,
		nexts:   make([]*virtualNode, rLevel+1),
		nodeIDs: []string{nodeID},
	}

	// 层数从高到低
	move := s.root
	for level := rLevel; level >= 0; level-- {
		for move.nexts[level] != nil && move.nexts[level].score < score {
			move = move.nexts[level]
		}
		newNode.nexts[level] = move.nexts[level]
		move.nexts[level] = &newNode
	}

	return nil
}

func (s *SkiplistHashRing) Ceiling(ctx context.Context, score int32) (int32, error) {
	target, ok := s.ceiling(score)
	if ok {
		return target, nil
	}

	first, _ := s.first()
	return first, nil
}

func (s *SkiplistHashRing) Floor(ctx context.Context, score int32) (int32, error) {
	target, ok := s.floor(score)
	if ok {
		return target, nil
	}

	last, _ := s.last()
	return last, nil
}

func (s *SkiplistHashRing) Rem(ctx context.Context, score int32, nodeID string) error {
	targetNode, ok := s.get(score)
	if !ok {
		return fmt.Errorf("score: %d not exist", score)
	}

	index := -1
	for i := 0; i < len(targetNode.nodeIDs); i++ {
		if targetNode.nodeIDs[i] == nodeID {
			index = i
			break
		}
	}

	if index == -1 {
		return fmt.Errorf("node: %s not exist in score: %d", nodeID, score)
	}

	delete(s.nodeToDataKey, nodeID)

	if len(targetNode.nodeIDs) > 1 {
		targetNode.nodeIDs = append(targetNode.nodeIDs[:index], targetNode.nodeIDs[index+1:]...)
		return nil
	}

	// 层数从高到低
	move := s.root
	for level := len(s.root.nexts) - 1; level >= 0; level-- {
		for move.nexts[level] != nil && move.nexts[level].score < score {
			move = move.nexts[level]
		}
		if move.nexts[level] == nil || move.nexts[level].score > score {
			continue
		}
		move.nexts[level] = move.nexts[level].nexts[level]
	}

	for level := 0; level < len(s.root.nexts); level++ {
		if s.root.nexts[level] != nil {
			continue
		}
		s.root.nexts = s.root.nexts[:level]
		break
	}

	return nil
}

func (s *SkiplistHashRing) Nodes(ctx context.Context) (map[string]int, error) {
	return s.nodeToReplicas, nil
}

func (s *SkiplistHashRing) AddNodeToReplica(ctx context.Context, nodeID string, replicas int) error {
	s.nodeToReplicas[nodeID] = replicas
	return nil
}

func (s *SkiplistHashRing) DeleteNodeToReplica(ctx context.Context, nodeID string) error {
	delete(s.nodeToReplicas, nodeID)
	return nil
}

func (s *SkiplistHashRing) Node(ctx context.Context, score int32) ([]string, error) {
	targetNode, ok := s.get(score)
	if !ok {
		return nil, fmt.Errorf("score: %d not exist", score)
	}
	return targetNode.nodeIDs, nil
}

func (s *SkiplistHashRing) DataKeys(ctx context.Context, nodeID string) (map[string]struct{}, error) {
	return s.nodeToDataKey[nodeID], nil
}

func (s *SkiplistHashRing) AddNodeToDataKeys(ctx context.Context, nodeID string, dataKeys map[string]struct{}) error {
	oldDataKeys := s.nodeToDataKey[nodeID]
	if oldDataKeys == nil {
		oldDataKeys = make(map[string]struct{})
	}
	for _dataKey := range dataKeys {
		oldDataKeys[_dataKey] = struct{}{}
	}
	s.nodeToDataKey[nodeID] = oldDataKeys
	return nil
}

func (s *SkiplistHashRing) DeleteNodeToDataKeys(ctx context.Context, nodeID string, dataKeys map[string]struct{}) error {
	oldDataKeys := s.nodeToDataKey[nodeID]
	if oldDataKeys == nil {
		return nil
	}
	for dataKey := range dataKeys {
		delete(oldDataKeys, dataKey)
	}
	if len(oldDataKeys) == 0 {
		delete(s.nodeToDataKey, nodeID)
	}
	return nil
}

func (s *SkiplistHashRing) roll() int {
	rander := rand.New(rand.NewSource(time.Now().UnixNano()))
	var level int
	for rander.Intn(2) == 1 {
		level++
	}
	return level
}

// 获得 >= score 且最接近 score 的目标
func (s *SkiplistHashRing) ceiling(score int32) (int32, bool) {
	if len(s.root.nexts) == 0 {
		return -1, false
	}

	move := s.root
	for level := len(s.root.nexts) - 1; level >= 0; level-- {
		for move.nexts[level] != nil && move.nexts[level].score < score {
			move = move.nexts[level]
		}
	}

	if move.nexts[0] == nil {
		return -1, false
	}

	return move.nexts[0].score, true
}

func (s *SkiplistHashRing) first() (int32, bool) {
	if len(s.root.nexts) == 0 {
		return -1, false
	}

	return s.root.nexts[0].score, true
}

func (s *SkiplistHashRing) floor(score int32) (int32, bool) {
	if len(s.root.nexts) == 0 {
		return -1, false
	}

	move := s.root
	for level := len(s.root.nexts) - 1; level >= 0; level-- {
		for move.nexts[level] != nil && move.nexts[level].score < score {
			move = move.nexts[level]
		}
	}

	if move.nexts[0] != nil && move.nexts[0].score == score {
		return score, true
	}

	if move == s.root {
		return -1, false
	}

	return move.score, true
}

// 返回最大的节点
func (s *SkiplistHashRing) last() (int32, bool) {
	// 层数从高到低
	move := s.root
	for level := len(s.root.nexts) - 1; level >= 0; level-- {
		for move.nexts[level] != nil {
			move = move.nexts[level]
		}
	}

	if move == s.root {
		return -1, false
	}

	return move.score, true
}

func (s *SkiplistHashRing) get(score int32) (*virtualNode, bool) {
	move := s.root
	for level := len(s.root.nexts) - 1; level >= 0; level-- {
		for move.nexts[level] != nil && move.nexts[level].score < score {
			move = move.nexts[level]
		}

		if move.nexts[level] != nil && move.nexts[level].score == score {
			return move.nexts[level], true
		}
	}

	return nil, false
}
