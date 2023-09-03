package consistent_hash

import "context"

type HashRing interface {
	Lock(ctx context.Context, expireSeconds int) error
	Unlock(ctx context.Context) error
	Add(ctx context.Context, virtualScore int32, nodeID string) error
	Ceiling(ctx context.Context, virtualScore int32) (int32, error)
	Floor(ctx context.Context, virtualScore int32) (int32, error)
	Rem(ctx context.Context, virtualScore int32, nodeID string) error
	Nodes(ctx context.Context) (map[string]int, error)
	AddNodeToReplica(ctx context.Context, nodeID string, replicas int) error
	DeleteNodeToReplica(ctx context.Context, nodeID string) error
	Node(ctx context.Context, virtualScore int32) ([]string, error)
	DataKeys(ctx context.Context, nodeID string) (map[string]struct{}, error)
	AddNodeToDataKeys(ctx context.Context, nodeID string, dataKeys map[string]struct{}) error
	DeleteNodeToDataKeys(ctx context.Context, nodeID string, dataKeys map[string]struct{}) error
}
