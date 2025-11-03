package recovery

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

const (
	componentRecoveryStorage = "recovery-storage"

	recoveryStorageStatePersistRecovering = "persist-recovering"
	recoveryStorageStateStreamRecovering  = "stream-recovering"
	recoveryStorageStateWorking           = "working"
)

// RecoverRecoveryStorage creates a new recovery storage.
func RecoverRecoveryStorage(
	ctx context.Context,
	recoveryStreamBuilder RecoveryStreamBuilder,
	lastTimeTickMessage message.ImmutableMessage,
) (RecoveryStorage, *RecoverySnapshot, error) {
	rs := newRecoveryStorage(recoveryStreamBuilder.Channel())
	// TODO 这里面要 obersrver switch mq的 消息，然后 flush 所有segment。确保segment 不跨 mq 【第二次进入的时候，从ccatalog里面拿到了 持久化的SN端的 checkpoint 是我们 switch mq的点位】
	// TODO:COMMENT_TO_REMOVE 先从meta恢复之前的recovery snapshot信息
	if err := rs.recoverRecoveryInfoFromMeta(ctx, recoveryStreamBuilder.Channel(), lastTimeTickMessage); err != nil {
		rs.Logger().Warn("recovery storage failed", zap.Error(err))
		return nil, nil, err
	}
	// TODO 这里应该会知道 最后一个 switch MQ的消息标志。【第一次应该在这里发现 switch MQ msg 】
	// TODO:COMMENT_TO_REMOVE 然后从上一次pchan consume checkpoint开始读取 WAL 继续恢复， 恢复到刚刚写入的lastTimeTickMessage。【打开之前，先send一个tt就是 last tt msg】
	// recover the state from wal and start the background task to persist the state.
	snapshot, err := rs.recoverFromStream(ctx, recoveryStreamBuilder, lastTimeTickMessage)
	if err != nil {
		rs.Logger().Warn("recovery storage failed", zap.Error(err))
		return nil, nil, err
	}
	// recovery storage start work.
	rs.metrics.ObserveStateChange(recoveryStorageStateWorking)
	rs.SetLogger(resource.Resource().Logger().With(
		zap.Int64("nodeID", paramtable.GetNodeID()),
		log.FieldComponent(componentRecoveryStorage),
		zap.String("channel", recoveryStreamBuilder.Channel().String()),
		zap.String("state", recoveryStorageStateWorking)))
	rs.truncator = newSamplingTruncator(
		snapshot.Checkpoint.Clone(),
		recoveryStreamBuilder.RWWALImpls(),
		rs.metrics,
	)
	rs.truncator.SetLogger(rs.Logger())
	go rs.backgroundTask()
	return rs, snapshot, nil
}

// newRecoveryStorage creates a new recovery storage.
func newRecoveryStorage(channel types.PChannelInfo) *recoveryStorageImpl {
	cfg := newConfig()
	return &recoveryStorageImpl{
		backgroundTaskNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		cfg:                    cfg,
		mu:                     sync.Mutex{},
		currentClusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		channel:                channel,
		dirtyCounter:           0,
		persistNotifier:        make(chan struct{}, 1),
		gracefulClosed:         false,
		metrics:                newRecoveryStorageMetrics(channel),
	}
}

// recoveryStorageImpl is a component that manages the recovery info for the streaming service.
// It will consume the message from the wal, consume the message in wal, and update the checkpoint for it.
type recoveryStorageImpl struct {
	log.Binder
	backgroundTaskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	cfg                    *config
	mu                     sync.Mutex
	currentClusterID       string
	channel                types.PChannelInfo
	segments               map[int64]*segmentRecoveryInfo
	vchannels              map[string]*vchannelRecoveryInfo
	checkpoint             *WALCheckpoint // TODO:COMMENT_TO_REMOVE consume pchannel checkpoint
	dirtyCounter           int            // records the message count since last persist snapshot.
	// used to trigger the recovery persist operation.
	persistNotifier        chan struct{}
	gracefulClosed         bool
	truncator              *samplingTruncator
	metrics                *recoveryMetrics
	pendingPersistSnapshot *RecoverySnapshot
	// used to mark switch MQ msg found
	foundSwitchMQMsg bool
	targetMQ         string
	switchConfig     map[string]string
	// TODO:COMMENT_TO_REMOVE flushed checkpoint, get from truncator, used switchMQ only
	flusherCheckpoint *WALCheckpoint
}

// Metrics gets the metrics of the wal.
func (r *recoveryStorageImpl) Metrics() RecoveryMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()

	return RecoveryMetrics{
		RecoveryTimeTick: r.checkpoint.TimeTick,
	}
}

// UpdateFlusherCheckpoint updates the checkpoint of flusher.
// TODO: should be removed in future, after merge the flusher logic into recovery storage.
func (r *recoveryStorageImpl) UpdateFlusherCheckpoint(vchannel string, checkpoint *WALCheckpoint) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if vchannelInfo, ok := r.vchannels[vchannel]; ok {
		if err := vchannelInfo.UpdateFlushCheckpoint(checkpoint); err != nil {
			r.Logger().Warn("failed to update flush checkpoint", zap.Error(err))
			return
		}
		r.Logger().Info("update flush checkpoint", zap.String("vchannel", vchannel), zap.String("messageID", checkpoint.MessageID.String()), zap.Uint64("timeTick", checkpoint.TimeTick))
		return
	}
	r.Logger().Warn("vchannel not found", zap.String("vchannel", vchannel))
}

// GetSchema gets the schema of the collection at the given timetick.
func (r *recoveryStorageImpl) GetSchema(ctx context.Context, vchannel string, timetick uint64) (*schemapb.CollectionSchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if vchannelInfo, ok := r.vchannels[vchannel]; ok {
		_, schema := vchannelInfo.GetSchema(timetick)
		if schema == nil {
			return nil, errors.Errorf("critical error: schema not found, vchannel: %s, timetick: %d", vchannel, timetick)
		}
		return schema, nil
	}
	return nil, errors.Errorf("critical error: vchannel not found, vchannel: %s, timetick: %d", vchannel, timetick)
}

// ObserveMessage is called when a new message is observed.
func (r *recoveryStorageImpl) ObserveMessage(ctx context.Context, msg message.ImmutableMessage) error {
	if h := msg.BroadcastHeader(); h != nil {
		// TODO:COMMENT_TO_REMOVE 如果是broadcast msg，则幂等ack一遍
		if err := streaming.WAL().Broadcast().Ack(ctx, msg); err != nil {
			r.Logger().Warn("failed to ack broadcast message", zap.Error(err))
			return err
		}
	}
	// TODO:COMMENT_TO_REMOVE control vchan不会影响 seg、vchan meta，只是影响 执行时ddl/dcl顺序
	if funcutil.IsControlChannel(msg.VChannel()) && msg.MessageType() != message.MessageTypeAlterReplicateConfig {
		// message on control channel except AlterReplicateConfig message is just used to determine the DDL/DCL order,
		// will not affect the recovery storage, so skip it.
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// TODO:COMMENT_TO_REMOVE 其他DML/DDL逐个 msg 观察处理
	r.observeMessage(msg)
	return nil
}

// Close closes the recovery storage and wait the background task stop.
func (r *recoveryStorageImpl) Close() {
	r.backgroundTaskNotifier.Cancel()
	r.backgroundTaskNotifier.BlockUntilFinish()
	// Stop the truncator.
	r.truncator.Close()
	r.metrics.Close()
}

// notifyPersist notifies a persist operation.
func (r *recoveryStorageImpl) notifyPersist() {
	select {
	case r.persistNotifier <- struct{}{}:
	default:
	}
}

// consumeDirtySnapshot consumes the dirty state and returns a snapshot to persist.
// A snapshot is always a consistent state (fully consume a message or a txn message) of the recovery storage.
func (r *recoveryStorageImpl) consumeDirtySnapshot() *RecoverySnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dirtyCounter == 0 {
		return nil
	}

	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	vchannels := make(map[string]*streamingpb.VChannelMeta)
	for _, segment := range r.segments {
		// TODO:COMMENT_TO_REMOVE 是不是dirty的，是不是flushed 的。
		// TODO 【dirty表示 内存有更新，和etcd上已经不一致，需要持久化到etcd上。】
		// TODO 【sholdbeRemove表示 这个segment已经不在 growing状态了，移除内存中的数据结构】
		dirtySnapshot, shouldBeRemoved := segment.ConsumeDirtyAndGetSnapshot()
		if shouldBeRemoved {
			delete(r.segments, segment.meta.SegmentId)
		}
		if dirtySnapshot != nil {
			segments[segment.meta.SegmentId] = dirtySnapshot
		}
	}
	for _, vchannel := range r.vchannels {
		// TODO:COMMENT_TO_REMOVE 是不是drop的vchan
		dirtySnapshot, shouldBeRemoved := vchannel.ConsumeDirtyAndGetSnapshot()
		if shouldBeRemoved {
			delete(r.vchannels, vchannel.meta.Vchannel)
		}
		if dirtySnapshot != nil {
			vchannels[vchannel.meta.Vchannel] = dirtySnapshot
		}
	}
	// clear the dirty counter.
	r.dirtyCounter = 0
	return &RecoverySnapshot{
		VChannels:          vchannels,
		SegmentAssignments: segments,
		Checkpoint:         r.checkpoint.Clone(),
	}
}

// observeMessage observes a message and update the recovery storage.
func (r *recoveryStorageImpl) observeMessage(msg message.ImmutableMessage) {
	if msg.TimeTick() <= r.checkpoint.TimeTick {
		if r.Logger().Level().Enabled(zap.DebugLevel) {
			r.Logger().Debug("skip the message before the checkpoint",
				log.FieldMessage(msg),
				zap.Uint64("checkpoint", r.checkpoint.TimeTick),
				zap.Uint64("incoming", msg.TimeTick()),
			)
		}
		return
	}
	// TODO:COMMENT_TO_REMOVE 先处理msg 观察逻辑
	r.handleMessage(msg)

	// TODO:COMMENT_TO_REMOVE 再更新checkpoint的内存数据结构，从而整体形成 snapshot {seg/vc meta+checkpoint}
	r.updateCheckpoint(msg)
	r.metrics.ObServeInMemMetrics(r.checkpoint.TimeTick)

	if !msg.IsPersisted() {
		// only trigger persist when the message is persisted.
		return
	}
	r.dirtyCounter++
	if r.dirtyCounter > r.cfg.maxDirtyMessages {
		r.notifyPersist()
	}
}

// updateCheckpoint updates the checkpoint of the recovery storage.
func (r *recoveryStorageImpl) updateCheckpoint(msg message.ImmutableMessage) {
	if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
		cfg := message.MustAsImmutableAlterReplicateConfigMessageV2(msg)
		r.checkpoint.ReplicateConfig = cfg.Header().ReplicateConfiguration
		clusterRole := replicateutil.MustNewConfigHelper(r.currentClusterID, cfg.Header().ReplicateConfiguration).GetCurrentCluster()
		switch clusterRole.Role() {
		case replicateutil.RolePrimary:
			r.checkpoint.ReplicateCheckpoint = nil
		case replicateutil.RoleSecondary:
			// Update the replicate checkpoint if the cluster role is secondary.
			sourceClusterID := clusterRole.SourceCluster().GetClusterId()
			sourcePChannel := clusterRole.MustGetSourceChannel(r.channel.Name)
			if r.checkpoint.ReplicateCheckpoint == nil || r.checkpoint.ReplicateCheckpoint.ClusterID != sourceClusterID {
				r.checkpoint.ReplicateCheckpoint = &utility.ReplicateCheckpoint{
					ClusterID: sourceClusterID,
					PChannel:  sourcePChannel,
					MessageID: nil,
					TimeTick:  0,
				}
			}
		}
	}
	// TODO:COMMENT_TO_REMOVE 最后一个msg ID 和 tt
	r.checkpoint.MessageID = msg.LastConfirmedMessageID()
	r.checkpoint.TimeTick = msg.TimeTick()

	// update the replicate checkpoint.
	replicateHeader := msg.ReplicateHeader()
	if replicateHeader == nil {
		return
	}
	if r.checkpoint.ReplicateCheckpoint == nil {
		r.detectInconsistency(msg, "replicate checkpoint is nil when incoming replicate message")
		return
	}
	if replicateHeader.ClusterID != r.checkpoint.ReplicateCheckpoint.ClusterID {
		r.detectInconsistency(msg,
			"replicate header cluster id mismatch",
			zap.String("expected", r.checkpoint.ReplicateCheckpoint.ClusterID),
			zap.String("actual", replicateHeader.ClusterID))
		return
	}
	r.checkpoint.ReplicateCheckpoint.MessageID = replicateHeader.LastConfirmedMessageID
	r.checkpoint.ReplicateCheckpoint.TimeTick = replicateHeader.TimeTick
}

// The incoming message id is always sorted with timetick.
func (r *recoveryStorageImpl) handleMessage(msg message.ImmutableMessage) {
	if msg.VChannel() != "" && msg.MessageType() != message.MessageTypeAlterWAL && msg.MessageType() != message.MessageTypeCreateCollection &&
		msg.MessageType() != message.MessageTypeDropCollection && r.vchannels[msg.VChannel()] == nil && !funcutil.IsControlChannel(msg.VChannel()) {
		r.detectInconsistency(msg, "vchannel not found")
	}

	switch msg.MessageType() {
	case message.MessageTypeInsert:
		immutableMsg := message.MustAsImmutableInsertMessageV1(msg)
		r.handleInsert(immutableMsg)
	case message.MessageTypeDelete:
		immutableMsg := message.MustAsImmutableDeleteMessageV1(msg)
		r.handleDelete(immutableMsg)
	case message.MessageTypeCreateSegment:
		immutableMsg := message.MustAsImmutableCreateSegmentMessageV2(msg)
		r.handleCreateSegment(immutableMsg)
	case message.MessageTypeFlush:
		immutableMsg := message.MustAsImmutableFlushMessageV2(msg)
		r.handleFlush(immutableMsg)
	case message.MessageTypeManualFlush:
		immutableMsg := message.MustAsImmutableManualFlushMessageV2(msg)
		r.handleManualFlush(immutableMsg)
	case message.MessageTypeCreateCollection:
		immutableMsg := message.MustAsImmutableCreateCollectionMessageV1(msg)
		r.handleCreateCollection(immutableMsg)
	case message.MessageTypeDropCollection:
		immutableMsg := message.MustAsImmutableDropCollectionMessageV1(msg)
		r.handleDropCollection(immutableMsg)
	case message.MessageTypeCreatePartition:
		immutableMsg := message.MustAsImmutableCreatePartitionMessageV1(msg)
		r.handleCreatePartition(immutableMsg)
	case message.MessageTypeDropPartition:
		immutableMsg := message.MustAsImmutableDropPartitionMessageV1(msg)
		r.handleDropPartition(immutableMsg)
	case message.MessageTypeTxn:
		immutableMsg := message.AsImmutableTxnMessage(msg)
		r.handleTxn(immutableMsg)
	case message.MessageTypeImport:
		immutableMsg := message.MustAsImmutableImportMessageV1(msg)
		r.handleImport(immutableMsg)
	case message.MessageTypeSchemaChange:
		immutableMsg := message.MustAsImmutableSchemaChangeMessageV2(msg)
		r.handleSchemaChange(immutableMsg)
	case message.MessageTypeTimeTick:
		// nothing, the time tick message make no recovery operation.
	case message.MessageTypeAlterWAL:
		immutableMsg := message.MustAsImmutableAlterWALMessageV1(msg)
		r.handleAlterWAL(immutableMsg)
	}
}

// handleAlterWAL handles the switch MQ message.
// When switching MQ, we need to flush all growing segments to ensure that segment data does not span across different MQ implementations.
func (r *recoveryStorageImpl) handleAlterWAL(msg message.ImmutableAlterWALMessageV1) {
	header := msg.Header()

	// Collect all growing segments that need to be flushed
	growingSegmentIDs := make([]int64, 0)
	segmentRows := make([]uint64, 0)
	segmentBinarySizes := make([]uint64, 0)

	// Iterate through all segments and flush growing ones
	for segmentID, segment := range r.segments {
		if segment.IsGrowing() {
			// Flush the growing segment to ensure data doesn't span across MQ switch
			segment.ObserveFlush(msg.TimeTick())

			growingSegmentIDs = append(growingSegmentIDs, segmentID)
			segmentRows = append(segmentRows, segment.Rows())
			segmentBinarySizes = append(segmentBinarySizes, segment.BinarySize())
		}
	}

	if len(growingSegmentIDs) > 0 {
		r.Logger().Info("SWITCH_MQ_STEPS: flush all growing segments for MQ switch",
			log.FieldMessage(msg),
			zap.String("targetMQ", header.TargetMq),
			zap.Int("flushedSegmentCount", len(growingSegmentIDs)),
			zap.Int64s("segmentIDs", growingSegmentIDs),
			zap.Uint64s("segmentRows", segmentRows),
			zap.Uint64s("segmentBinarySizes", segmentBinarySizes))
	} else {
		r.Logger().Info("SWITCH_MQ_STEPS: no growing segments to flush for MQ switch",
			log.FieldMessage(msg),
			zap.String("targetMQ", header.TargetMq))
	}

	// Mark that we have found a switch MQ message and record the switch information
	// This will be persisted in the snapshot to ensure recovery after restart
	r.foundSwitchMQMsg = true
	r.targetMQ = header.TargetMq
	r.switchConfig = header.Config

	r.Logger().Info("SWITCH_MQ_STEPS: switch MQ information recorded",
		zap.Bool("foundSwitchMQMsg", r.foundSwitchMQMsg),
		zap.String("targetMQ", r.targetMQ),
		zap.Any("switchConfig", r.switchConfig))
}

// handleInsert handles the insert message.
func (r *recoveryStorageImpl) handleInsert(msg message.ImmutableInsertMessageV1) {
	// TODO:COMMENT_TO_REMOVE 一个insert msg可能写入到多个 partition中的多个segments中
	for _, partition := range msg.Header().GetPartitions() {
		if segment, ok := r.segments[partition.SegmentAssignment.SegmentId]; ok && segment.IsGrowing() {
			// TODO:COMMENT_TO_REMOVE 更新每个seg的 统计信息
			segment.ObserveInsert(msg.TimeTick(), partition)
			if r.Logger().Level().Enabled(zap.DebugLevel) {
				r.Logger().Debug("insert entity", log.FieldMessage(msg), zap.Uint64("segmentRows", segment.Rows()), zap.Uint64("segmentBinary", segment.BinarySize()))
			}
		} else {
			r.detectInconsistency(msg, "segment not found")
		}
	}
}

// handleDelete handles the delete message.
func (r *recoveryStorageImpl) handleDelete(msg message.ImmutableDeleteMessageV1) {
	// nothing, current delete operation is managed by flowgraph, not recovery storage.
	if r.Logger().Level().Enabled(zap.DebugLevel) {
		r.Logger().Debug("delete entity", log.FieldMessage(msg))
	}
}

// handleCreateSegment handles the create segment message.
func (r *recoveryStorageImpl) handleCreateSegment(msg message.ImmutableCreateSegmentMessageV2) {
	// TODO:COMMENT_TO_REMOVE 创建segment meta、stats 内存记录
	segment := newSegmentRecoveryInfoFromCreateSegmentMessage(msg)
	r.segments[segment.meta.SegmentId] = segment
	r.Logger().Info("create segment", log.FieldMessage(msg))
}

// handleFlush handles the flush message.
func (r *recoveryStorageImpl) handleFlush(msg message.ImmutableFlushMessageV2) {
	header := msg.Header()
	if segment, ok := r.segments[header.SegmentId]; ok {
		// TODO:COMMENT_TO_REMOVE 记录这个segment state的flushed状态和 flush checkpoint tt
		segment.ObserveFlush(msg.TimeTick())
		r.Logger().Info("flush segment", log.FieldMessage(msg), zap.Uint64("rows", segment.Rows()), zap.Uint64("binarySize", segment.BinarySize()))
	}
}

// handleManualFlush handles the manual flush message.
func (r *recoveryStorageImpl) handleManualFlush(msg message.ImmutableManualFlushMessageV2) {
	segments := make(map[int64]struct{}, len(msg.Header().SegmentIds))
	for _, segmentID := range msg.Header().SegmentIds {
		segments[segmentID] = struct{}{}
	}
	r.flushSegments(msg, segments)
}

// flushSegments flushes the segments in the recovery storage.
func (r *recoveryStorageImpl) flushSegments(msg message.ImmutableMessage, sealSegmentIDs map[int64]struct{}) {
	segmentIDs := make([]int64, 0)
	rows := make([]uint64, 0)
	binarySize := make([]uint64, 0)
	for _, segment := range r.segments {
		if _, ok := sealSegmentIDs[segment.meta.SegmentId]; ok {
			segment.ObserveFlush(msg.TimeTick())
			segmentIDs = append(segmentIDs, segment.meta.SegmentId)
			rows = append(rows, segment.Rows())
			binarySize = append(binarySize, segment.BinarySize())
		}
	}
	if len(segmentIDs) != len(sealSegmentIDs) {
		r.detectInconsistency(msg, "flush segments not exist", zap.Int64s("wanted", lo.Keys(sealSegmentIDs)), zap.Int64s("actually", segmentIDs))
	}
	r.Logger().Info("flush all segments of collection by manual flush", log.FieldMessage(msg), zap.Uint64s("rows", rows), zap.Uint64s("binarySize", binarySize))
}

// handleCreateCollection handles the create collection message.
func (r *recoveryStorageImpl) handleCreateCollection(msg message.ImmutableCreateCollectionMessageV1) {
	// TODO:COMMENT_TO_REMOVE 幂等创建collection的vchan, create collection的时候，分配的每个vchannel 都会收到一个 create collection信息的。所以每个msg里面其实只有一个vchannel。
	if _, ok := r.vchannels[msg.VChannel()]; ok {
		return
	}
	r.vchannels[msg.VChannel()] = newVChannelRecoveryInfoFromCreateCollectionMessage(msg)
	r.Logger().Info("create collection", log.FieldMessage(msg))
}

// handleDropCollection handles the drop collection message.
func (r *recoveryStorageImpl) handleDropCollection(msg message.ImmutableDropCollectionMessageV1) {
	// TODO:COMMENT_TO_REMOVE 和create collection msg同理
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; !ok || vchannelInfo.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		return
	}
	r.vchannels[msg.VChannel()].ObserveDropCollection(msg)
	// flush all existing segments.
	// TODO:COMMENT_TO_REMOVE 找到当前collection所有segment，都mark flushed一下
	r.flushAllSegmentOfCollection(msg, msg.Header().CollectionId)
	r.Logger().Info("drop collection", log.FieldMessage(msg))
}

// flushAllSegmentOfCollection flushes all segments of the collection.
func (r *recoveryStorageImpl) flushAllSegmentOfCollection(msg message.ImmutableMessage, collectionID int64) {
	segmentIDs := make([]int64, 0)
	rows := make([]uint64, 0)
	for _, segment := range r.segments {
		if segment.meta.CollectionId == collectionID {
			segment.ObserveFlush(msg.TimeTick())
			segmentIDs = append(segmentIDs, segment.meta.SegmentId)
			rows = append(rows, segment.Rows())
		}
	}
	r.Logger().Info("flush all segments of collection", log.FieldMessage(msg), zap.Int64s("segmentIDs", segmentIDs), zap.Uint64s("rows", rows))
}

// handleCreatePartition handles the create partition message.
func (r *recoveryStorageImpl) handleCreatePartition(msg message.ImmutableCreatePartitionMessageV1) {
	// TODO:COMMENT_TO_REMOVE partition和collection一个意思，创建partition的时候会向 collection所有的vchannel发送一个 partition create msg信息
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; !ok || vchannelInfo.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		return
	}
	r.vchannels[msg.VChannel()].ObserveCreatePartition(msg)
	r.Logger().Info("create partition", log.FieldMessage(msg))
}

// handleDropPartition handles the drop partition message.
func (r *recoveryStorageImpl) handleDropPartition(msg message.ImmutableDropPartitionMessageV1) {
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; !ok || vchannelInfo.meta.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
		// TODO: drop partition should never happen after the drop collection message.
		// But now we don't have strong promise on it.
		return
	}
	r.vchannels[msg.VChannel()].ObserveDropPartition(msg)
	// flush all existing segments.
	r.flushAllSegmentOfPartition(msg, msg.Header().PartitionId)
	r.Logger().Info("drop partition", log.FieldMessage(msg))
}

// flushAllSegmentOfPartition flushes all segments of the partition.
func (r *recoveryStorageImpl) flushAllSegmentOfPartition(msg message.ImmutableMessage, partitionID int64) {
	segmentIDs := make([]int64, 0)
	rows := make([]uint64, 0)
	for _, segment := range r.segments {
		if segment.meta.PartitionId == partitionID {
			segment.ObserveFlush(msg.TimeTick())
			segmentIDs = append(segmentIDs, segment.meta.SegmentId)
			rows = append(rows, segment.Rows())
		}
	}
	r.Logger().Info("flush all segments of partition", log.FieldMessage(msg), zap.Int64s("segmentIDs", segmentIDs), zap.Uint64s("rows", rows))
}

// handleTxn handles the txn message.
func (r *recoveryStorageImpl) handleTxn(msg message.ImmutableTxnMessage) {
	msg.RangeOver(func(im message.ImmutableMessage) error {
		r.handleMessage(im)
		return nil
	})
}

// handleImport handles the import message.
func (r *recoveryStorageImpl) handleImport(_ message.ImmutableImportMessageV1) {
}

// handleSchemaChange handles the schema change message.
func (r *recoveryStorageImpl) handleSchemaChange(msg message.ImmutableSchemaChangeMessageV2) {
	// TODO:COMMENT_TO_REMOVE schema变化的时候，也会向所有vchan发生信息。这就先要把现有segment 都flush
	// when schema change happens, we need to flush all segments in the collection.
	segments := make(map[int64]struct{}, len(msg.Header().FlushedSegmentIds))
	for _, segmentID := range msg.Header().FlushedSegmentIds {
		segments[segmentID] = struct{}{}
	}
	r.flushSegments(msg, segments)

	// TODO:COMMENT_TO_REMOVE 然后重新更新内存中vchannel 对应关联的 collection schema history历史。这个schemas
	// persist the schema change into recovery info.
	if vchannelInfo, ok := r.vchannels[msg.VChannel()]; ok {
		vchannelInfo.ObserveSchemaChange(msg)
	}
}

// detectInconsistency detects the inconsistency in the recovery storage.
func (r *recoveryStorageImpl) detectInconsistency(msg message.ImmutableMessage, reason string, extra ...zap.Field) {
	fields := make([]zap.Field, 0, len(extra)+2)
	fields = append(fields, log.FieldMessage(msg), zap.String("reason", reason))
	fields = append(fields, extra...)
	// The log is not fatal in some cases.
	// because our meta is not atomic-updated, so these error may be logged if crashes when meta updated partially.
	r.Logger().Warn("inconsistency detected", fields...)
	r.metrics.ObserveInconsitentEvent()
}

func (r *recoveryStorageImpl) GetFlusherCheckpointAdv() *WALCheckpoint {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.vchannels) == 0 {
		r.Logger().Info("SWITCH_MQ_STEPS: get flush checkpoint fast return pChan cp, due to no vChan", zap.String("pChannel", r.channel.String()))
		return r.checkpoint
	}

	var minimumCheckpoint *WALCheckpoint
	for _, vchannel := range r.vchannels {
		if vchannel.GetFlushCheckpoint() == nil {
			// If any flush checkpoint is not set, not ready.
			return nil
		}
		if minimumCheckpoint == nil || vchannel.GetFlushCheckpoint().TimeTick < minimumCheckpoint.TimeTick {
			minimumCheckpoint = vchannel.GetFlushCheckpoint()
		}
	}
	return minimumCheckpoint
}

// GetFlusherCheckpoint returns flusher checkpoint concurrent-safe
// NOTE: shall not be called with r.mu.Lock()!
func (r *recoveryStorageImpl) GetFlusherCheckpoint() *WALCheckpoint {
	r.mu.Lock()
	defer r.mu.Unlock()

	var minimumCheckpoint *WALCheckpoint
	for _, vchannel := range r.vchannels {
		if vchannel.GetFlushCheckpoint() == nil {
			// If any flush checkpoint is not set, not ready.
			return nil
		}
		if minimumCheckpoint == nil || vchannel.GetFlushCheckpoint().MessageID.LTE(minimumCheckpoint.MessageID) {
			minimumCheckpoint = vchannel.GetFlushCheckpoint()
		}
	}
	return minimumCheckpoint
}
