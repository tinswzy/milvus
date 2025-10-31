package adaptor

import (
	"context"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher/flusherimpl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ wal.Opener = (*openerAdaptorImpl)(nil)

// Test only
// Deprecated: Use NewOpenerAdaptor instead.
func adaptImplsToOpener(basicOpener walimpls.OpenerImpls, interceptorBuilders ...interceptors.InterceptorBuilder) wal.Opener {
	o := &openerAdaptorImpl{
		lifetime:            typeutil.NewLifetime(),
		openerCache:         make(map[message.WALName]walimpls.OpenerImpls),
		idAllocator:         typeutil.NewIDAllocator(),
		walInstances:        typeutil.NewConcurrentMap[int64, wal.WAL](),
		interceptorBuilders: nil,
	}
	o.openerCache[message.WALNameTest] = basicOpener
	return o
}

// NewOpenerAdaptor creates a new dynamic wal opener that can open different MQ types at runtime.
// It doesn't bind to a specific walName at construction time, instead it selects the appropriate
// wal implementation based on the walName in OpenOption when Open() is called.
func NewOpenerAdaptor(builders []interceptors.InterceptorBuilder) wal.Opener {
	o := &openerAdaptorImpl{
		lifetime:            typeutil.NewLifetime(),
		openerCache:         make(map[message.WALName]walimpls.OpenerImpls),
		idAllocator:         typeutil.NewIDAllocator(),
		walInstances:        typeutil.NewConcurrentMap[int64, wal.WAL](),
		interceptorBuilders: builders,
	}
	o.SetLogger(resource.Resource().Logger().With(log.FieldComponent("wal-opener")))
	return o
}

// openerAdaptorImpl is the wrapper that adapts walimpls.OpenerImpls to wal.Opener.
// It supports opening different MQ types dynamically at runtime.
type openerAdaptorImpl struct {
	log.Binder

	lifetime            *typeutil.Lifetime
	mu                  sync.RWMutex                             // protects openerCache
	openerCache         map[message.WALName]walimpls.OpenerImpls // cache of opened walimpls.OpenerImpls, dynamically created based on walName
	idAllocator         *typeutil.IDAllocator
	walInstances        *typeutil.ConcurrentMap[int64, wal.WAL] // store all wal instances allocated by these allocator.
	interceptorBuilders []interceptors.InterceptorBuilder
}

// Open opens a wal instance for the channel.
// It dynamically selects the MQ implementation based on opt.WALName.
func (o *openerAdaptorImpl) Open(ctx context.Context, opt *wal.OpenOption) (wal.WAL, error) {
	if !o.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal opener is on shutdown")
	}
	defer o.lifetime.Done()

	// Determine which walName to use
	walName := message.WALNameUnknown
	catalog := resource.Resource().StreamingNodeCatalog()
	cpProto, err := catalog.GetConsumeCheckpoint(ctx, opt.Channel.Name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get checkpoint from catalog")
	}
	if cpProto != nil {
		checkpoint := utility.NewWALCheckpointFromProto(cpProto)
		originalWalName := walName.String()
		walName = checkpoint.MessageID.WALName()
		if checkpoint.IsSwitchMqMsg {
			walName = message.NewWALName(checkpoint.TargetMq)
			o.Logger().Info("SWITCH_MQ_STEPS: get checkpoint from catalog",
				zap.String("checkpoint", checkpoint.MessageID.String()),
				zap.Uint64("timetick", checkpoint.TimeTick),
				zap.String("targetMq", checkpoint.TargetMq),
				zap.String("originalWalName", originalWalName),
			)
		}
	}

	if walName == message.WALNameUnknown {
		// If not specified, use default from config
		walName = util.MustSelectWALName()
	}

	logger := o.Logger().With(
		zap.String("channel", opt.Channel.String()),
		zap.Stringer("walName", walName),
	)

	// Get or create the underlying walimpls.OpenerImpls for this walName
	openerImpl, err := o.getOrCreateOpenerImpl(walName)
	if err != nil {
		logger.Warn("get or create underlying wal impls opener failed", zap.Error(err))
		return nil, err
	}

	// Open the underlying WAL implementation
	l, err := openerImpl.Open(ctx, &walimpls.OpenOption{
		Channel: opt.Channel,
	})
	if err != nil {
		logger.Warn("open wal impls failed", zap.Error(err))
		return nil, err
	}

	var wal wal.WAL
	switch opt.Channel.AccessMode {
	case types.AccessModeRW:
		wal, err = o.openRWWAL(ctx, l, opt)
	case types.AccessModeRO:
		wal, err = o.openROWAL(l)
	default:
		panic("unknown access mode")
	}
	if err != nil {
		logger.Warn("open wal failed", zap.Error(err))
		return nil, err
	}
	logger.Info("open wal done", zap.Stringer("walName", walName))
	return wal, nil
}

// getOrCreateOpenerImpl gets an existing walimpls.OpenerImpls from cache or creates a new one.
func (o *openerAdaptorImpl) getOrCreateOpenerImpl(walName message.WALName) (walimpls.OpenerImpls, error) {
	// Fast path: read lock
	o.mu.RLock()
	if opener, ok := o.openerCache[walName]; ok {
		o.mu.RUnlock()
		return opener, nil
	}
	o.mu.RUnlock()

	// Slow path: write lock and create
	o.mu.Lock()
	defer o.mu.Unlock()

	// Double-check after acquiring write lock
	if opener, ok := o.openerCache[walName]; ok {
		return opener, nil
	}

	// Get builder from registry and build opener
	builder := registry.MustGetBuilder(walName)
	opener, err := builder.Build()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to build walimpls opener for %s", walName)
	}

	// Cache the opener
	o.openerCache[walName] = opener
	o.Logger().Info("SWITCH_MQ_STEPS: created and cached new walimpls opener", zap.Stringer("walName", walName))
	return opener, nil
}

// openRWWAL opens a read write wal instance for the channel.
func (o *openerAdaptorImpl) openRWWAL(ctx context.Context, l walimpls.WALImpls, opt *wal.OpenOption) (wal.WAL, error) {
	id := o.idAllocator.Allocate()
	roWAL := adaptImplsToROWAL(l, func() {
		o.walInstances.Remove(id)
	})

	// recover the wal state.
	param, err := buildInterceptorParams(ctx, l)
	if err != nil {
		roWAL.Close()
		return nil, errors.Wrap(err, "when building interceptor params")
	}
	// TODO 要挪到前面去，先获取 rs from meta&streaming 才能决定要open什么wal
	rs, snapshot, err := recovery.RecoverRecoveryStorage(ctx, newRecoveryStreamBuilder(roWAL), param.LastTimeTickMessage)
	if err != nil {
		param.Clear()
		roWAL.Close()
		return nil, errors.Wrap(err, "when recovering recovery storage")
	}

	// snapshot倒数第二条是 switch mq的点位，而且checkpoint不是 switch mq点位时，说明checkpoint到 switch mq消息之间还有数据没有flush，需要显式flush掉
	if snapshot.FoundSwitchMQMsg {
		return o.handleSwitchMQ(ctx, l, opt, roWAL, param, rs, snapshot)
	}

	param.InitialRecoverSnapshot = snapshot
	param.TxnManager = txn.NewTxnManager(param.ChannelInfo, snapshot.TxnBuffer.GetUncommittedMessageBuilder())
	param.ShardManager = shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo:            param.ChannelInfo,
		WAL:                    param.WAL,
		InitialRecoverSnapshot: snapshot,
		TxnManager:             param.TxnManager,
	})
	if param.ReplicateManager, err = replicates.RecoverReplicateManager(
		&replicates.ReplicateManagerRecoverParam{
			ChannelInfo:            param.ChannelInfo,
			CurrentClusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
			InitialRecoverSnapshot: snapshot,
		},
	); err != nil {
		return nil, err
	}

	// TODO 第三次再进来的时候，强制从 snapshot里面拿到哪个 checkpoint点位，而不是从mixcoord里面那那个 datanode flusher的点位。
	// TODO 因为 flush数据还是用的 老的 datanode的那些逻辑，所以这里的checkpoint有两个点位，一个是streamingnode的，一个是datanode老那套的。老那套的 点位都是通过coord来获取的。

	// start the flusher to flush and generate recovery info.
	var flusher *flusherimpl.WALFlusherImpl
	if !opt.DisableFlusher {
		// TODO:COMMENT_TO_REMOVE 构建一个异步flush开始 读wal 分发vchan到data sync service 落盘
		flusher = flusherimpl.RecoverWALFlusher(&flusherimpl.RecoverWALFlusherParam{
			WAL:              param.WAL,
			RecoveryStorage:  rs,
			ChannelInfo:      l.Channel(),
			RecoverySnapshot: snapshot,
		})
	}

	// TODO:COMMENT_TO_REMOVE 构建wal adaptor 准备接收日志写入
	wal := adaptImplsToRWWAL(roWAL, o.interceptorBuilders, param, flusher)
	o.walInstances.Insert(id, wal)
	return wal, nil
}

// Handle MQ switch: if snapshot has switch MQ flag, we need to:
// 1. All growing segments have been marked as FLUSHED in recovery storage (handleSwitchMQType)
// 2. Close recovery storage to persist the final snapshot with updated checkpoint
// 3. Clean up resources
// 4. Return error to trigger WAL re-opening from the new checkpoint with new MQ type
func (o *openerAdaptorImpl) handleSwitchMQ(ctx context.Context, l walimpls.WALImpls, opt *wal.OpenOption,
	roWAL *roWALAdaptorImpl, param *interceptors.InterceptorBuildParam, rs recovery.RecoveryStorage, snapshot *recovery.RecoverySnapshot,
) (wal.WAL, error) {
	logger := o.Logger().With(
		zap.String("channel", opt.Channel.String()),
		zap.String("targetMQ", snapshot.TargetMQ),
		zap.Bool("foundSwitchMQMsg", snapshot.FoundSwitchMQMsg))

	logger.Info("SWITCH_MQ_STEPS: detected MQ switch message in snapshot",
		zap.String("switchCheckpoint", snapshot.Checkpoint.MessageID.String()),
		zap.Uint64("switchTimeTick", snapshot.Checkpoint.TimeTick),
		zap.Any("switchConfig", snapshot.SwitchConfig))

	// flush all segments
	var flusher *flusherimpl.WALFlusherImpl
	if !opt.DisableFlusher {
		flusher = flusherimpl.RecoverWALFlusher(&flusherimpl.RecoverWALFlusherParam{
			WAL:              param.WAL,
			RecoveryStorage:  rs,
			ChannelInfo:      l.Channel(),
			RecoverySnapshot: snapshot,
		})
	}

	defer func() {
		// Close recovery storage to persist the final state
		// This ensures the checkpoint and segment states (FLUSHED) are persisted to etcd
		logger.Info("SWITCH_MQ_STEPS: closing recovery storage to persist MQ switch snapshot")
		rs.Close()
		// Clean up resources
		flusher.Close()
		param.Clear()
		roWAL.Close()
	}()

	// Wait for all data to be flushed before closing the WAL
	// We need to ensure that all vchannel data up to the LastTimeTickMessage has been persisted
	targetTimeTick := param.LastTimeTickMessage.TimeTick()
	logger.Info("SWITCH_MQ_STEPS: waiting for all data to be flushed for MQ switch",
		zap.Uint64("targetTimeTick", targetTimeTick))

	// Use ticker to periodically check flush progress
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Set timeout to avoid infinite waiting (default 5 minutes)
	const defaultMQSwitchFlushTimeout = 1 * time.Minute
	checkCtx, checkCancel := context.WithTimeout(context.Background(), defaultMQSwitchFlushTimeout)
	defer checkCancel()

	// Periodically check if flusher checkpoint has reached the target timetick
	for {
		select {
		case <-ticker.C:
			flusherCP := rs.GetFlusherCheckpointAdv()
			if flusherCP == nil {
				logger.Info("SWITCH_MQ_STEPS: MQ switch: waiting for flusher checkpoint to be initialized")
				continue
			}

			if flusherCP.TimeTick >= targetTimeTick {
				logger.Info("SWITCH_MQ_STEPS: MQ switch: all data flushed successfully, ready to switch MQ, start closing WAL to trigger re-opening with new MQ type",
					zap.Uint64("flusherCheckpointTimeTick", flusherCP.TimeTick),
					zap.Uint64("targetTimeTick", targetTimeTick),
					zap.String("flusherCheckpointMsgID", flusherCP.MessageID.String()))

				// Return specific error to indicate MQ switch is needed
				// The WAL manager will catch this error and re-open the WAL with new MQ type
				// On next open, the new MQ implementation will be used starting from the switch checkpoint
				logger.Info("SWITCH_MQ_STEPS: MQ switch preparation completed, returning error to trigger WAL re-opening with new MQ",
					zap.String("newMQType", snapshot.TargetMQ),
					zap.String("newCheckpoint", snapshot.Checkpoint.MessageID.String()))
				return nil, errors.Errorf(
					"SWITCH_MQ_STEPS:MQ switch detected: switch to %s at checkpoint %s (timetick: %d), WAL needs to be re-opened with new MQ type",
					snapshot.TargetMQ,
					snapshot.Checkpoint.MessageID.String(),
					snapshot.Checkpoint.TimeTick)
			}

			// Print progress log
			remaining := targetTimeTick - flusherCP.TimeTick
			logger.Info("SWITCH_MQ_STEPS:MQ switch: waiting for data flush completion",
				zap.Uint64("currentFlusherCheckpointTimeTick", flusherCP.TimeTick),
				zap.Uint64("targetTimeTick", targetTimeTick),
				zap.Uint64("remainingTimeTick", remaining),
				zap.String("currentFlusherCheckpointMsgID", flusherCP.MessageID.String()))

		case <-checkCtx.Done():
			logger.Warn("SWITCH_MQ_STEPS:MQ switch: timeout waiting for data flush completion",
				zap.Error(checkCtx.Err()),
				zap.Duration("timeout", defaultMQSwitchFlushTimeout))
			return nil, errors.Wrap(checkCtx.Err(), "timeout waiting for flush completion during MQ switch")

		case <-ctx.Done():
			logger.Warn("SWITCH_MQ_STEPS:MQ switch: context canceled while waiting for data flush completion",
				zap.Error(ctx.Err()))
			return nil, errors.Wrap(ctx.Err(), "context canceled during MQ switch flush waiting")
		}
	}
}

// openROWAL opens a read only wal instance for the channel.
func (o *openerAdaptorImpl) openROWAL(l walimpls.WALImpls) (wal.WAL, error) {
	id := o.idAllocator.Allocate()
	wal := adaptImplsToROWAL(l, func() {
		o.walInstances.Remove(id)
	})
	o.walInstances.Insert(id, wal)
	return wal, nil
}

// Close the wal opener, release the underlying resources.
func (o *openerAdaptorImpl) Close() {
	o.lifetime.SetState(typeutil.LifetimeStateStopped)
	o.lifetime.Wait()

	o.Logger().Info("wal opener closing...")

	// close all wal instances.
	o.walInstances.Range(func(id int64, l wal.WAL) bool {
		l.Close()
		o.Logger().Info("close wal by opener", zap.Int64("id", id), zap.String("channel", l.Channel().String()))
		return true
	})

	// close all cached opener impls
	o.mu.Lock()
	defer o.mu.Unlock()
	for walName, opener := range o.openerCache {
		o.Logger().Info("closing underlying walimpls opener", zap.Stringer("walName", walName))
		opener.Close()
	}
	o.openerCache = nil

	o.Logger().Info("wal opener closed")
}
