package flusherimpl

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var errChannelLifetimeUnrecoverable = errors.New("channel lifetime unrecoverable")

// RecoverWALFlusherParam is the parameter for building wal flusher.
type RecoverWALFlusherParam struct {
	ChannelInfo      types.PChannelInfo
	WAL              *syncutil.Future[wal.WAL]
	RecoverySnapshot *recovery.RecoverySnapshot
	RecoveryStorage  recovery.RecoveryStorage
}

// RecoverWALFlusher recovers the wal flusher.
func RecoverWALFlusher(param *RecoverWALFlusherParam) *WALFlusherImpl {
	flusher := &WALFlusherImpl{
		notifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		wal:      param.WAL,
		logger: resource.Resource().Logger().With(
			log.FieldComponent("flusher"),
			zap.String("pchannel", param.ChannelInfo.String())),
		metrics:         newFlusherMetrics(param.ChannelInfo),
		RecoveryStorage: param.RecoveryStorage,
	}
	go flusher.Execute(param.RecoverySnapshot)
	return flusher
}

type WALFlusherImpl struct {
	notifier          *syncutil.AsyncTaskNotifier[struct{}]
	wal               *syncutil.Future[wal.WAL]
	flusherComponents *flusherComponents
	logger            *log.MLogger
	metrics           *flusherMetrics
	recovery.RecoveryStorage
}

// Execute starts the wal flusher.
func (impl *WALFlusherImpl) Execute(recoverSnapshot *recovery.RecoverySnapshot) (err error) {
	defer func() {
		impl.notifier.Finish(struct{}{})
		if err == nil {
			impl.logger.Info("wal flusher stop")
			return
		}
		if !errors.Is(err, context.Canceled) {
			impl.logger.DPanic("wal flusher stop to executing with unexpected error", zap.Error(err))
			return
		}
		impl.logger.Warn("wal flusher is canceled before executing", zap.Error(err))
	}()

	impl.logger.Info("wal flusher start to recovery...")
	l, err := impl.wal.GetWithContext(impl.notifier.Context())
	if err != nil {
		return errors.Wrap(err, "when get wal from future")
	}
	impl.logger.Info("wal ready for flusher recovery")

	var checkpoint message.MessageID
	// TODO:COMMENT_TO_REMOVE 先构建flusher data sync service组件包装，flusherComponent 包含了 map<vchannelName,data sync service> 管理
	impl.flusherComponents, checkpoint, err = impl.buildFlusherComponents(impl.notifier.Context(), l, recoverSnapshot)
	if err != nil {
		return errors.Wrap(err, "when build flusher components")
	}
	defer impl.flusherComponents.Close()

	// TODO:COMMENT_TO_REMOVE 从pchannel consume checkpoint开始 读日志
	// TODO:COMMENT_TO_REMOVE 这里可能需要我们修改下，如果switch mq之后，这个flusher 第二次应该从哪里启动、用什么创建client等? 不过不需要其实，因为open wal的时候会根据checkpoint来打开client。
	scanner, err := impl.generateScanner(impl.notifier.Context(), impl.wal.Get(), checkpoint)
	if err != nil {
		return errors.Wrap(err, "when generate scanner")
	}
	defer scanner.Close()

	impl.logger.Info("wal flusher start to work")
	impl.metrics.IntoState(flusherStateInWorking)
	defer impl.metrics.IntoState(flusherStateOnClosing)

	for {
		select {
		case <-impl.notifier.Context().Done():
			return nil
		case msg, ok := <-scanner.Chan():
			if !ok {
				impl.logger.Warn("wal flusher is closing for closed scanner channel, which is unexpected at graceful way")
				return nil
			}
			impl.metrics.ObserveMetrics(msg.TimeTick())
			// TODO:COMMENT_TO_REMOVE 每个日志分发到对应的data sync service
			if err := impl.dispatch(msg); err != nil {
				// The error is always context canceled.
				return nil
			}
		}
	}
}

// Close closes the wal flusher and release all related resources for it.
func (impl *WALFlusherImpl) Close() {
	impl.notifier.Cancel()
	impl.notifier.BlockUntilFinish()

	impl.logger.Info("wal flusher start to close the recovery storage...")
	impl.RecoveryStorage.Close()
	impl.logger.Info("recovery storage closed")

	impl.metrics.Close()
}

// buildFlusherComponents builds the components of the flusher.
func (impl *WALFlusherImpl) buildFlusherComponents(ctx context.Context, l wal.WAL, snapshot *recovery.RecoverySnapshot) (*flusherComponents, message.MessageID, error) {
	// Get all existed vchannels of the pchannel.
	vchannels := lo.Keys(snapshot.VChannels)
	impl.logger.Info("SWITCH_MQ_STEPS: fetch vchannel done", zap.Int("vchannelNum", len(vchannels)))

	// Get all the recovery info of the recoverable vchannels.
	recoverInfos, checkpoint, err := impl.getRecoveryInfos(ctx, vchannels)
	if err != nil {
		impl.logger.Warn("get recovery info failed", zap.Error(err))
		return nil, nil, err
	}
	impl.logger.Info("fetch recovery info done", zap.Int("recoveryInfoNum", len(recoverInfos)))
	// TODO:COMMENT_TO_REMOVE 在第三次进入的时候，应该检查这个checkpoint就是 那个switchMQ的msg 点位，那么就要切换为对应目标mq的 第0个 id
	if len(vchannels) == 0 && checkpoint == nil {
		impl.logger.Info("no vchannel to recover, use the snapshot checkpoint", zap.Stringer("checkpoint", snapshot.Checkpoint.MessageID))
		checkpoint = snapshot.Checkpoint.MessageID
	}

	mixc, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		impl.logger.Warn("flusher recovery is canceled before data coord client ready", zap.Error(err))
		return nil, nil, err
	}
	impl.logger.Info("data coord client ready")

	// build all components.
	broker := broker.NewCoordBroker(mixc, paramtable.GetNodeID())
	chunkManager := resource.Resource().ChunkManager()

	cpUpdater := util.NewChannelCheckpointUpdaterWithCallback(broker, func(mp *msgpb.MsgPosition) { // TODO:COMMENT_TO_REMOVE flush vchannel 落盘后的 定时 放入 更新 vchan flushed checkpoint点位 task
		messageID := adaptor.MustGetMessageIDFromMQWrapperIDBytes(mp.MsgID)
		impl.RecoveryStorage.UpdateFlusherCheckpoint(mp.ChannelName, &recovery.WALCheckpoint{
			MessageID: messageID,
			TimeTick:  mp.Timestamp,
			Magic:     utility.RecoveryMagicStreamingInitialized,
		})
	})
	go cpUpdater.Start()

	fc := &flusherComponents{
		wal:                        l,
		broker:                     broker,
		cpUpdater:                  cpUpdater, // TODO:COMMENT_TO_REMOVE 放这个 cpUpdater进去，让后面data sync service创建的ttNode能够用到这个 cpUpdater
		chunkManager:               chunkManager,
		dataServices:               make(map[string]*dataSyncServiceWrapper),
		logger:                     impl.logger,
		recoveryCheckPointTimeTick: snapshot.Checkpoint.TimeTick,
		rs:                         impl.RecoveryStorage,
	}
	impl.logger.Info("flusher components intiailizing done")
	if err := fc.recover(ctx, recoverInfos); err != nil {
		impl.logger.Warn("flusher recovery is canceled before recovery done, recycle the resource", zap.Error(err))
		fc.Close()
		impl.logger.Info("flusher recycle the resource done")
		return nil, nil, err
	}
	impl.logger.Info("SWITCH_MQ_STEPS: flusher recovery done",
		zap.String("pchannel", l.Channel().Name),
		zap.String("pcWalType", l.WALName().String()),
		zap.String("flusherCpWAType", checkpoint.WALName().String()),
		zap.String("flusherCp", checkpoint.String()))
	for k, v := range recoverInfos {
		impl.logger.Info("SWITCH_MQ_STEPS: flusher recover info",
			zap.String("vchannel", k),
			zap.Any("recoverInfo", v.Info))
	}
	return fc, checkpoint, nil
}

// generateScanner create a new scanner for the wal.
func (impl *WALFlusherImpl) generateScanner(ctx context.Context, l wal.WAL, checkpoint message.MessageID) (wal.Scanner, error) {
	handler := make(adaptor.ChanMessageHandler, 64)
	readOpt := wal.ReadOption{
		VChannel:       "", // We need consume all message from wal.
		MesasgeHandler: handler,
		DeliverPolicy:  options.DeliverPolicyAll(),
	}
	if checkpoint != nil {
		impl.logger.Info("wal start to scan from minimum checkpoint", zap.Stringer("checkpointMessageID", checkpoint))
		readOpt.DeliverPolicy = options.DeliverPolicyStartFrom(checkpoint)
	} else {
		impl.logger.Info("wal start to scan from the earliest checkpoint")
	}
	return l.Read(ctx, readOpt)
}

// dispatch dispatches the message to the related handler for flusher components.
func (impl *WALFlusherImpl) dispatch(msg message.ImmutableMessage) (err error) {
	// TODO: We will merge the flusher into recovery storage in future.
	// Currently, flusher works as a separate component.
	defer func() {
		if err = impl.RecoveryStorage.ObserveMessage(impl.notifier.Context(), msg); err != nil {
			impl.logger.Warn("failed to observe message", zap.Error(err))
		}
	}()

	// wal flusher will not handle the control channel message.
	if funcutil.IsControlChannel(msg.VChannel()) {
		return nil
	}

	// TODO:COMMENT_TO_REMOVE 每个消息属于vchan级别，一个pchan这里会读到各种vchan的msg
	// Do the data sync service management here.
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		createCollectionMsg, err := message.AsImmutableCreateCollectionMessageV1(msg)
		if err != nil {
			impl.logger.DPanic("the message type is not CreateCollectionMessage", zap.Error(err))
			return nil
		}
		// TODO:COMMENT_TO_REMOVE 主要是rs oberserve create create 到这个vchan，然后就是创建 vchan对应的data sync service组件
		impl.flusherComponents.WhenCreateCollection(createCollectionMsg)
	case message.MessageTypeDropCollection:
		// defer to remove the data sync service from the components.
		// TODO: Current drop collection message will be handled by the underlying data sync service.
		// TODO:COMMENT_TO_REMOVE 主要是移除 vchan对应的 data sync service组件
		defer func() {
			impl.flusherComponents.WhenDropCollection(msg.VChannel())
		}()
	}
	// TODO:COMMENT_TO_REMOVE 核心处理各种 DML/DDL
	return impl.flusherComponents.HandleMessage(impl.notifier.Context(), msg)
}
