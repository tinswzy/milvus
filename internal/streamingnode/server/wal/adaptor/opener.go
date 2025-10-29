package adaptor

import (
	"context"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher/flusherimpl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ wal.Opener = (*openerAdaptorImpl)(nil)

// adaptImplsToOpener creates a new wal opener with opener impls.
func adaptImplsToOpener(opener walimpls.OpenerImpls, builders []interceptors.InterceptorBuilder) wal.Opener {
	o := &openerAdaptorImpl{
		lifetime:            typeutil.NewLifetime(),
		opener:              opener,
		idAllocator:         typeutil.NewIDAllocator(),
		walInstances:        typeutil.NewConcurrentMap[int64, wal.WAL](),
		interceptorBuilders: builders,
	}
	o.SetLogger(resource.Resource().Logger().With(log.FieldComponent("wal-opener")))
	return o
}

// openerAdaptorImpl is the wrapper of OpenerImpls to Opener.
type openerAdaptorImpl struct {
	log.Binder

	lifetime            *typeutil.Lifetime
	opener              walimpls.OpenerImpls
	idAllocator         *typeutil.IDAllocator
	walInstances        *typeutil.ConcurrentMap[int64, wal.WAL] // store all wal instances allocated by these allocator.
	interceptorBuilders []interceptors.InterceptorBuilder
}

// Open opens a wal instance for the channel.
func (o *openerAdaptorImpl) Open(ctx context.Context, opt *wal.OpenOption) (wal.WAL, error) {
	if !o.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal opener is on shutdown")
	}
	defer o.lifetime.Done()

	logger := o.Logger().With(zap.String("channel", opt.Channel.String()))

	l, err := o.opener.Open(ctx, &walimpls.OpenOption{
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
	logger.Info("open wal done")
	return wal, nil
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
	rs, snapshot, err := recovery.RecoverRecoveryStorage(ctx, newRecoveryStreamBuilder(roWAL), param.LastTimeTickMessage)
	if err != nil {
		param.Clear()
		roWAL.Close()
		return nil, errors.Wrap(err, "when recovering recovery storage")
	}

	// Handle MQ switch: if snapshot has switch MQ flag
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
	roWAL *roWALAdaptorImpl, param *interceptors.InterceptorBuildParam, rs recovery.RecoveryStorage, snapshot *recovery.RecoverySnapshot) (wal.WAL, error) {
	logger := o.Logger().With(
		zap.String("channel", opt.Channel.String()),
		zap.String("targetMQ", snapshot.TargetMQ),
		zap.Bool("foundSwitchMQMsg", snapshot.FoundSwitchMQMsg))

	logger.Info("detected MQ switch message in snapshot",
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
		logger.Info("closing recovery storage to persist MQ switch snapshot")
		rs.Close()
		// Clean up resources
		flusher.Close()
		param.Clear()
		roWAL.Close()
	}()

	// Return specific error to indicate MQ switch is needed
	// The WAL manager will catch this error and re-open the WAL with new MQ type
	// On next open, the new MQ implementation will be used starting from the switch checkpoint
	logger.Info("MQ switch preparation completed, returning error to trigger WAL re-opening with new MQ",
		zap.String("newMQType", snapshot.TargetMQ),
		zap.String("newCheckpoint", snapshot.Checkpoint.MessageID.String()))

	// Wait for all data to be flushed before closing the WAL
	// We need to ensure that all vchannel data up to the LastTimeTickMessage has been persisted
	targetTimeTick := param.LastTimeTickMessage.TimeTick()
	logger.Info("waiting for all data to be flushed for MQ switch",
		zap.Uint64("targetTimeTick", targetTimeTick))

	// Use ticker to periodically check flush progress
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Set timeout to avoid infinite waiting (default 5 minutes)
	const defaultMQSwitchFlushTimeout = 5 * time.Minute
	checkCtx, checkCancel := context.WithTimeout(ctx, defaultMQSwitchFlushTimeout)
	defer checkCancel()

	// Periodically check if flusher checkpoint has reached the target timetick
	for {
		select {
		case <-ticker.C:
			flusherCP := rs.GetFlusherCheckpoint()
			if flusherCP == nil {
				logger.Info("MQ switch: waiting for flusher checkpoint to be initialized")
				continue
			}

			if flusherCP.TimeTick >= targetTimeTick {
				logger.Info("MQ switch: all data flushed successfully, ready to switch MQ",
					zap.Uint64("flusherCheckpointTimeTick", flusherCP.TimeTick),
					zap.Uint64("targetTimeTick", targetTimeTick),
					zap.String("flusherCheckpointMsgID", flusherCP.MessageID.String()))
				logger.Info("MQ switch: all existing data has been flushed, closing WAL to trigger re-opening with new MQ type")

				return nil, errors.Errorf(
					"MQ switch detected: switch to %s at checkpoint %s (timetick: %d), WAL needs to be re-opened with new MQ type",
					snapshot.TargetMQ,
					snapshot.Checkpoint.MessageID.String(),
					snapshot.Checkpoint.TimeTick)
			}

			// Print progress log
			remaining := targetTimeTick - flusherCP.TimeTick
			logger.Info("MQ switch: waiting for data flush completion",
				zap.Uint64("currentFlusherCheckpointTimeTick", flusherCP.TimeTick),
				zap.Uint64("targetTimeTick", targetTimeTick),
				zap.Uint64("remainingTimeTick", remaining),
				zap.String("currentFlusherCheckpointMsgID", flusherCP.MessageID.String()))

		case <-checkCtx.Done():
			logger.Warn("MQ switch: timeout waiting for data flush completion",
				zap.Error(checkCtx.Err()),
				zap.Duration("timeout", defaultMQSwitchFlushTimeout))
			return nil, errors.Wrap(checkCtx.Err(), "timeout waiting for flush completion during MQ switch")

		case <-ctx.Done():
			logger.Warn("MQ switch: context canceled while waiting for data flush completion",
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

	// close all wal instances.
	o.walInstances.Range(func(id int64, l wal.WAL) bool {
		l.Close()
		o.Logger().Info("close wal by opener", zap.Int64("id", id), zap.String("channel", l.Channel().String()))
		return true
	})
	// close the opener
	o.opener.Close()
}
