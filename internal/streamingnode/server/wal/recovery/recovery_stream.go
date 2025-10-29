package recovery

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// recoverFromStream recovers the recovery storage from the recovery stream.
func (r *recoveryStorageImpl) recoverFromStream(
	ctx context.Context,
	recoveryStreamBuilder RecoveryStreamBuilder,
	lastTimeTickMessage message.ImmutableMessage,
) (snapshot *RecoverySnapshot, err error) {
	r.metrics.ObserveStateChange(recoveryStorageStateStreamRecovering)
	r.metrics.ObServePersistedMetrics(r.checkpoint.TimeTick)
	r.SetLogger(resource.Resource().Logger().With(
		log.FieldComponent(componentRecoveryStorage),
		zap.String("channel", recoveryStreamBuilder.Channel().String()),
		zap.String("startMessageID", r.checkpoint.MessageID.String()),
		zap.Uint64("fromTimeTick", r.checkpoint.TimeTick),
		zap.Uint64("toTimeTick", lastTimeTickMessage.TimeTick()),
		zap.String("state", recoveryStorageStateStreamRecovering),
	))

	r.Logger().Info("recover from wal stream...")
	// TODO:COMMENT_TO_REMOVE 开始异步 recovery 读 WAL，从 checkpoint位置读到 last tt msg为止
	rs := recoveryStreamBuilder.Build(BuildRecoveryStreamParam{
		StartCheckpoint: r.checkpoint.MessageID,
		EndTimeTick:     lastTimeTickMessage.TimeTick(),
	})
	defer func() {
		rs.Close()
		if err != nil {
			r.Logger().Warn("recovery from wal stream failed", zap.Error(err))
			return
		}
		r.Logger().Info("recovery from wal stream done",
			zap.Int("vchannels", len(snapshot.VChannels)),
			zap.Int("segments", len(snapshot.SegmentAssignments)),
			zap.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
			zap.Uint64("timetick", snapshot.Checkpoint.TimeTick),
		)
	}()
L:
	// TODO:COMMENT_TO_REMOVE 持续观察读到的msg，解析msg并执行相应的 seg meta、vchan meta、checkpoint等信息更新。
	for {
		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "failed to recover from wal")
		case msg, ok := <-rs.Chan():
			if !ok {
				// The recovery stream is reach the end, we can stop the recovery.
				break L
			}
			r.ObserveMessage(ctx, msg)
		}
	}
	if rs.Error() != nil {
		return nil, errors.Wrap(rs.Error(), "failed to read the recovery info from wal")
	}

	// TODO:COMMENT_TO_REMOVE 恢复完一次了，把recovery快照返回，供上层持久化，避免下次再重复恢复这一端wal数据。
	snapshot = r.getSnapshot()
	snapshot.TxnBuffer = rs.TxnBuffer()
	return snapshot, nil
}

// getSnapshot returns the snapshot of the recovery storage.
// Use this function to get the snapshot after recovery is finished,
// and use the snapshot to recover all write ahead components.
func (r *recoveryStorageImpl) getSnapshot() *RecoverySnapshot {
	segments := make(map[int64]*streamingpb.SegmentAssignmentMeta, len(r.segments))
	vchannels := make(map[string]*streamingpb.VChannelMeta, len(r.vchannels))
	for segmentID, segment := range r.segments {
		// TODO:COMMENT_TO_REMOVE 只关注 growing的，因为sealed的太多，也不会变，不需要管其实。
		if segment.IsGrowing() {
			segments[segmentID] = proto.Clone(segment.meta).(*streamingpb.SegmentAssignmentMeta)
		}
	}
	for channelName, vchannel := range r.vchannels {
		// TODO:COMMENT_TO_REMOVE 只需要关注 active的，非active的已经不需要管了
		if vchannel.IsActive() {
			vchannels[channelName] = proto.Clone(vchannel.meta).(*streamingpb.VChannelMeta)
		}
	}
	return &RecoverySnapshot{
		VChannels:          vchannels,
		SegmentAssignments: segments,
		Checkpoint:         r.checkpoint.Clone(),
		FoundSwitchMQMsg:   r.foundSwitchMQMsg,
		TargetMQ:           r.targetMQ,
		SwitchConfig:       r.switchConfig,
	}
}
