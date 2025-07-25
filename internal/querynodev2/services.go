// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querynodev2

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/querynodev2/tasks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// GetComponentStates returns information about whether the node is healthy
func (node *QueryNode) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	stats := &milvuspb.ComponentStates{
		Status: merr.Success(),
	}

	code := node.lifetime.GetState()
	nodeID := common.NotRegisteredID

	if node.session != nil && node.session.Registered() {
		nodeID = node.GetNodeID()
	}
	log.Ctx(ctx).Debug("QueryNode current state", zap.Int64("NodeID", nodeID), zap.String("StateCode", code.String()))

	info := &milvuspb.ComponentInfo{
		NodeID:    nodeID,
		Role:      typeutil.QueryNodeRole,
		StateCode: code,
	}
	stats.State = info
	return stats, nil
}

// GetTimeTickChannel returns the time tick channel
// TimeTickChannel contains many time tick messages, which will be sent by query nodes
func (node *QueryNode) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  paramtable.Get().CommonCfg.QueryCoordTimeTick.GetValue(),
	}, nil
}

// GetStatisticsChannel returns the statistics channel
// Statistics channel contains statistics infos of query nodes, such as segment infos, memory infos
func (node *QueryNode) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
	}, nil
}

// GetStatistics returns loaded statistics of collection.
func (node *QueryNode) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) (*internalpb.GetStatisticsResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		zap.Strings("vChannels", req.GetDmlChannels()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("timeTravel", req.GetReq().GetTravelTimestamp()),
	)
	log.Debug("received GetStatisticsRequest")

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &internalpb.GetStatisticsResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	failRet := &internalpb.GetStatisticsResponse{
		Status: merr.Success(),
	}

	var toReduceResults []*internalpb.GetStatisticsResponse
	var mu sync.Mutex
	runningGp, runningCtx := errgroup.WithContext(ctx)
	for _, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.GetStatisticsRequest{
			Req:             req.Req,
			DmlChannels:     []string{ch},
			SegmentIDs:      req.SegmentIDs,
			FromShardLeader: req.FromShardLeader,
			Scope:           req.Scope,
		}
		runningGp.Go(func() error {
			ret, err := node.getChannelStatistics(runningCtx, req, ch)
			if err == nil {
				err = merr.Error(ret.GetStatus())
			}

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failRet.Status = merr.Status(err)
				return err
			}
			toReduceResults = append(toReduceResults, ret)
			return nil
		})
	}
	if err := runningGp.Wait(); err != nil {
		return failRet, nil
	}

	ret, err := reduceStatisticResponse(toReduceResults)
	if err != nil {
		failRet.Status = merr.Status(err)
		return failRet, nil
	}
	log.Debug("reduce statistic result done")

	return ret, nil
}

func (node *QueryNode) composeIndexMeta(ctx context.Context, indexInfos []*indexpb.IndexInfo, schema *schemapb.CollectionSchema) *segcorepb.CollectionIndexMeta {
	fieldIndexMetas := make([]*segcorepb.FieldIndexMeta, 0)
	for _, info := range indexInfos {
		fieldIndexMetas = append(fieldIndexMetas, &segcorepb.FieldIndexMeta{
			CollectionID:    info.GetCollectionID(),
			FieldID:         info.GetFieldID(),
			IndexName:       info.GetIndexName(),
			TypeParams:      info.GetTypeParams(),
			IndexParams:     info.GetIndexParams(),
			IsAutoIndex:     info.GetIsAutoIndex(),
			UserIndexParams: info.GetUserIndexParams(),
		})
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(schema)
	maxIndexRecordPerSegment := int64(0)
	if err != nil || sizePerRecord == 0 {
		log.Ctx(ctx).Warn("failed to transfer segment size to collection, because failed to estimate size per record", zap.Error(err))
	} else {
		threshold := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024
		proportion := paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat()
		maxIndexRecordPerSegment = int64(threshold * proportion / float64(sizePerRecord))
	}

	return &segcorepb.CollectionIndexMeta{
		IndexMetas:       fieldIndexMetas,
		MaxIndexRowCount: maxIndexRecordPerSegment,
	}
}

// WatchDmChannels create consumers on dmChannels to receive Incremental data，which is the important part of real-time query
func (node *QueryNode) WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (status *commonpb.Status, e error) {
	defer node.updateDistributionModifyTS()

	channel := req.GetInfos()[0]
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("channel", channel.GetChannelName()),
		zap.Int64("currentNodeID", node.GetNodeID()),
	)

	log.Info("received watch channel request",
		zap.Int64("version", req.GetVersion()),
	)

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check index
	if len(req.GetIndexInfoList()) == 0 {
		err := merr.WrapErrIndexNotFoundForCollection(req.GetSchema().GetName())
		return merr.Status(err), nil
	}

	if !node.subscribingChannels.Insert(channel.GetChannelName()) {
		msg := "channel subscribing..."
		log.Warn(msg)
		return merr.Success(), nil
	}
	defer node.subscribingChannels.Remove(channel.GetChannelName())

	// to avoid concurrent watch/unwatch
	if node.unsubscribingChannels.Contain(channel.GetChannelName()) {
		err := merr.WrapErrChannelReduplicate(channel.GetChannelName(), "the other same channel is unsubscribing")
		log.Warn("failed to unsubscribe channel", zap.Error(err))
		return merr.Status(err), nil
	}

	_, exist := node.delegators.Get(channel.GetChannelName())
	if exist {
		log.Info("channel already subscribed")
		return merr.Success(), nil
	}

	err := node.manager.Collection.PutOrRef(req.GetCollectionID(), req.GetSchema(),
		node.composeIndexMeta(ctx, req.GetIndexInfoList(), req.Schema), req.GetLoadMeta())
	if err != nil {
		log.Warn("failed to ref collection", zap.Error(err))
		return merr.Status(err), nil
	}
	defer func() {
		if !merr.Ok(status) {
			node.manager.Collection.Unref(req.GetCollectionID(), 1)
		}
	}()

	queryView := delegator.NewChannelQueryView(
		channel.GetUnflushedSegmentIds(),
		req.GetSealedSegmentRowCount(),
		req.GetPartitionIDs(),
		req.GetTargetVersion(),
	)

	delegator, err := delegator.NewShardDelegator(
		ctx,
		req.GetCollectionID(),
		req.GetReplicaID(),
		channel.GetChannelName(),
		req.GetVersion(),
		node.clusterManager,
		node.manager,
		node.loader,
		node.factory,
		channel.GetSeekPosition().GetTimestamp(),
		node.queryHook,
		node.chunkManager,
		queryView,
	)
	if err != nil {
		log.Warn("failed to create shard delegator", zap.Error(err))
		return merr.Status(err), nil
	}
	node.delegators.Insert(channel.GetChannelName(), delegator)
	defer func() {
		if err != nil {
			node.delegators.GetAndRemove(channel.GetChannelName())
			delegator.Close()
		}
	}()

	pipeline, err := node.pipelineManager.Add(req.GetCollectionID(), channel.GetChannelName())
	if err != nil {
		msg := "failed to create pipeline"
		log.Warn(msg, zap.Error(err))
		return merr.Status(err), nil
	}
	defer func() {
		if err != nil {
			node.pipelineManager.Remove(channel.GetChannelName())
		}
	}()

	growingInfo := lo.SliceToMap(channel.GetUnflushedSegmentIds(), func(id int64) (int64, uint64) {
		info := req.GetSegmentInfos()[id]
		return id, info.GetDmlPosition().GetTimestamp()
	})
	delegator.AddExcludedSegments(growingInfo)

	flushedInfo := lo.SliceToMap(channel.GetFlushedSegmentIds(), func(id int64) (int64, uint64) {
		return id, typeutil.MaxTimestamp
	})
	delegator.AddExcludedSegments(flushedInfo)

	droppedInfo := lo.SliceToMap(channel.GetDroppedSegmentIds(), func(id int64) (int64, uint64) {
		return id, typeutil.MaxTimestamp
	})
	delegator.AddExcludedSegments(droppedInfo)

	defer func() {
		if err != nil {
			// remove legacy growing
			node.manager.Segment.RemoveBy(ctx, segments.WithChannel(channel.GetChannelName()),
				segments.WithType(segments.SegmentTypeGrowing))
		}
	}()

	err = loadL0Segments(ctx, delegator, req)
	if err != nil {
		log.Warn("failed to load l0 segments", zap.Error(err))
		return merr.Status(err), nil
	}
	err = loadGrowingSegments(ctx, delegator, req)
	if err != nil {
		msg := "failed to load growing segments"
		log.Warn(msg, zap.Error(err))
		return merr.Status(err), nil
	}

	var position *msgpb.MsgPosition
	deleteCheckpoint := channel.GetDeleteCheckpoint()
	channelCheckpoint := channel.GetSeekPosition()
	if deleteCheckpoint == nil {
		// for compatibility with old version coord, which doesn't have delete checkpoint in VchannelInfo
		log.Info("no delete checkpoint found, use seek position to seek",
			zap.Time("seekPosition", tsoutil.PhysicalTime(channelCheckpoint.GetTimestamp())),
		)
		position = &msgpb.MsgPosition{
			ChannelName: channelCheckpoint.GetChannelName(),
			MsgID:       channelCheckpoint.GetMsgID(),
			Timestamp:   channelCheckpoint.GetTimestamp(),
		}
	} else {
		if channelCheckpoint.GetTimestamp() > deleteCheckpoint.GetTimestamp() {
			msg := "channel seek position is greater than delete checkpoint, use delete checkpoint to seek"
			log.Info(msg,
				zap.Time("seekPosition", tsoutil.PhysicalTime(channelCheckpoint.GetTimestamp())),
				zap.Time("deleteCheckpoint", tsoutil.PhysicalTime(deleteCheckpoint.GetTimestamp())),
			)
			position = &msgpb.MsgPosition{
				ChannelName: deleteCheckpoint.GetChannelName(),
				MsgID:       deleteCheckpoint.GetMsgID(),
				Timestamp:   deleteCheckpoint.GetTimestamp(),
			}
		} else {
			msg := "channel seek position is smaller than delete checkpoint, use seek position to seek"
			log.Info(msg,
				zap.Time("seekPosition", tsoutil.PhysicalTime(channelCheckpoint.GetTimestamp())),
				zap.Time("deleteCheckpoint", tsoutil.PhysicalTime(deleteCheckpoint.GetTimestamp())),
			)
			position = &msgpb.MsgPosition{
				ChannelName: channelCheckpoint.GetChannelName(),
				MsgID:       channelCheckpoint.GetMsgID(),
				Timestamp:   channelCheckpoint.GetTimestamp(),
			}
		}
	}

	err = pipeline.ConsumeMsgStream(ctx, position)
	if err != nil {
		err = merr.WrapErrServiceUnavailable(err.Error(), "InitPipelineFailed")
		log.Warn(err.Error(),
			zap.Int64("collectionID", channel.CollectionID),
			zap.String("channel", channel.ChannelName),
		)
		return merr.Status(err), nil
	}

	// start pipeline
	pipeline.Start()
	// delegator after all steps done
	delegator.Start()
	log.Info("watch dml channel success")
	return merr.Success(), nil
}

func (node *QueryNode) UnsubDmChannel(ctx context.Context, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error) {
	defer node.updateDistributionModifyTS()
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("channel", req.GetChannelName()),
		zap.Int64("currentNodeID", node.GetNodeID()),
	)

	log.Info("received unsubscribe channel request")

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	node.unsubscribingChannels.Insert(req.GetChannelName())
	defer node.unsubscribingChannels.Remove(req.GetChannelName())
	delegator, ok := node.delegators.GetAndRemove(req.GetChannelName())
	if ok {
		node.pipelineManager.Remove(req.GetChannelName())

		// close the delegator first to block all coming query/search requests
		delegator.Close()

		node.manager.Segment.RemoveBy(ctx, segments.WithChannel(req.GetChannelName()), segments.WithType(segments.SegmentTypeGrowing))
		node.manager.Collection.Unref(req.GetCollectionID(), 1)
	}
	log.Info("unsubscribed channel")

	return merr.Success(), nil
}

func (node *QueryNode) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64s("partitionIDs", req.GetPartitionIDs()),
	)

	log.Info("received load partitions request")
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	collection := node.manager.Collection.Get(req.GetCollectionID())
	if collection != nil {
		collection.AddPartition(req.GetPartitionIDs()...)
	}

	log.Info("load partitions done")
	return merr.Success(), nil
}

// LoadSegments load historical data into query node, historical data can be vector data or index
func (node *QueryNode) LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	defer node.updateDistributionModifyTS()
	segment := req.GetInfos()[0]

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", segment.GetCollectionID()),
		zap.Int64("partitionID", segment.GetPartitionID()),
		zap.String("shard", segment.GetInsertChannel()),
		zap.Int64("segmentID", segment.GetSegmentID()),
		zap.String("level", segment.GetLevel().String()),
		zap.Int64("currentNodeID", node.GetNodeID()),
		zap.Int64("dstNodeID", req.GetDstNodeID()),
	)

	log.Info("received load segments request",
		zap.Int64("version", req.GetVersion()),
		zap.Bool("needTransfer", req.GetNeedTransfer()),
		zap.String("loadScope", req.GetLoadScope().String()))
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// check index
	if len(req.GetIndexInfoList()) == 0 {
		err := merr.WrapErrIndexNotFoundForCollection(req.GetSchema().GetName())
		return merr.Status(err), nil
	}

	// fallback binlog memory size to log size when it is zero
	fallbackBinlogMemorySize := func(binlogs []*datapb.FieldBinlog) {
		for _, insertBinlogs := range binlogs {
			for _, b := range insertBinlogs.GetBinlogs() {
				if b.GetMemorySize() == 0 {
					b.MemorySize = b.GetLogSize()
				}
			}
		}
	}
	for _, s := range req.GetInfos() {
		fallbackBinlogMemorySize(s.GetBinlogPaths())
		fallbackBinlogMemorySize(s.GetStatslogs())
		fallbackBinlogMemorySize(s.GetDeltalogs())
	}

	// Delegates request to workers
	if req.GetNeedTransfer() {
		delegator, ok := node.delegators.Get(segment.GetInsertChannel())
		if !ok {
			msg := "failed to load segments, delegator not found"
			log.Warn(msg)
			err := merr.WrapErrChannelNotFound(segment.GetInsertChannel())
			return merr.Status(err), nil
		}

		if len(req.GetInfos()) > 0 && req.GetInfos()[0].Level == datapb.SegmentLevel_L0 {
			// force l0 segment to load on delegator
			if req.DstNodeID != node.GetNodeID() {
				log.Info("unexpected L0 segment load on non-delegator node, force to load on delegator")
				req.DstNodeID = node.GetNodeID()
			}
		}

		req.NeedTransfer = false
		err := delegator.LoadSegments(ctx, req)
		if err != nil {
			log.Warn("delegator failed to load segments", zap.Error(err))
			return merr.Status(err), nil
		}

		return merr.Success(), nil
	}

	err := node.manager.Collection.PutOrRef(req.GetCollectionID(), req.GetSchema(),
		node.composeIndexMeta(ctx, req.GetIndexInfoList(), req.GetSchema()), req.GetLoadMeta())
	if err != nil {
		log.Warn("failed to ref collection", zap.Error(err))
		return merr.Status(err), nil
	}
	defer node.manager.Collection.Unref(req.GetCollectionID(), 1)

	if req.GetLoadScope() == querypb.LoadScope_Delta {
		return node.loadDeltaLogs(ctx, req), nil
	}
	if req.GetLoadScope() == querypb.LoadScope_Index {
		return node.loadIndex(ctx, req), nil
	}
	if req.GetLoadScope() == querypb.LoadScope_Stats {
		return node.loadStats(ctx, req), nil
	}

	// Actual load segment
	log.Info("start to load segments...")
	loaded, err := node.loader.Load(ctx,
		req.GetCollectionID(),
		segments.SegmentTypeSealed,
		req.GetVersion(),
		req.GetInfos()...,
	)
	if err != nil {
		return merr.Status(err), nil
	}

	node.manager.Collection.Ref(req.GetCollectionID(), uint32(len(loaded)))

	log.Info("load segments done...",
		zap.Int64s("segments", lo.Map(loaded, func(s segments.Segment, _ int) int64 { return s.ID() })))

	return merr.Success(), nil
}

// UpdateSchema updates the schema of the collection on the querynode.
func (node *QueryNode) UpdateSchema(ctx context.Context, req *querypb.UpdateSchemaRequest) (*commonpb.Status, error) {
	defer node.updateDistributionModifyTS()

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Uint64("schemaVersion", req.GetVersion()),
	)

	log.Info("querynode received update schema request")

	err := node.manager.Collection.UpdateSchema(req.GetCollectionID(), req.GetSchema(), req.GetVersion())
	if err != nil {
		log.Warn("failed to update schema", zap.Error(err))
	}

	return merr.Status(err), nil
}

// ReleaseCollection clears all data related to this collection on the querynode
func (node *QueryNode) ReleaseCollection(ctx context.Context, in *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	return merr.Success(), nil
}

// ReleasePartitions clears all data related to this partition on the querynode
func (node *QueryNode) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collection", req.GetCollectionID()),
		zap.Int64s("partitions", req.GetPartitionIDs()),
	)

	log.Info("received release partitions request")

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	collection := node.manager.Collection.Get(req.GetCollectionID())
	if collection != nil {
		for _, partition := range req.GetPartitionIDs() {
			collection.RemovePartition(partition)
		}
	}

	log.Info("release partitions done")
	return merr.Success(), nil
}

// ReleaseSegments remove the specified segments from query node according segmentIDs, partitionIDs, and collectionID
func (node *QueryNode) ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	defer node.updateDistributionModifyTS()
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("shard", req.GetShard()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Int64("currentNodeID", node.GetNodeID()),
	)

	log.Info("received release segment request",
		zap.String("scope", req.GetScope().String()),
		zap.Bool("needTransfer", req.GetNeedTransfer()),
	)

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthyOrStopping); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	if req.GetNeedTransfer() {
		delegator, ok := node.delegators.Get(req.GetShard())
		if !ok {
			msg := "failed to release segment, delegator not found"
			log.Warn(msg)
			err := merr.WrapErrChannelNotFound(req.GetShard())
			return merr.Status(err), nil
		}

		req.NeedTransfer = false
		err := delegator.ReleaseSegments(ctx, req, false)
		if err != nil {
			log.Warn("delegator failed to release segment", zap.Error(err))
			return merr.Status(err), nil
		}

		return merr.Success(), nil
	}

	log.Info("start to release segments")
	sealedCount := 0
	for _, id := range req.GetSegmentIDs() {
		_, count := node.manager.Segment.Remove(ctx, id, req.GetScope())
		sealedCount += count
	}
	node.manager.Collection.Unref(req.GetCollectionID(), uint32(sealedCount))

	return merr.Success(), nil
}

// GetSegmentInfo returns segment information of the collection on the queryNode, and the information includes memSize, numRow, indexName, indexID ...
func (node *QueryNode) GetSegmentInfo(ctx context.Context, in *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &querypb.GetSegmentInfoResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	var segmentInfos []*querypb.SegmentInfo
	for _, segmentID := range in.GetSegmentIDs() {
		segment := node.manager.Segment.Get(segmentID)
		if segment == nil {
			continue
		}
		collection := node.manager.Collection.Get(segment.Collection())
		if collection == nil {
			continue
		}

		// TODO(yah01): now Milvus supports only 1 vector field
		vecFields := funcutil.GetVecFieldIDs(collection.Schema())
		var (
			indexName  string
			indexID    int64
			indexInfos []*querypb.FieldIndexInfo
		)
		for _, field := range vecFields {
			indexes := segment.GetIndex(field)
			if indexes != nil {
				if len(indexes) != 1 {
					log.Error("only support one index for vector field", zap.Int64("fieldID", field), zap.Int("index count", len(indexes)))
					return &querypb.GetSegmentInfoResponse{
						Status: merr.Status(merr.WrapErrServiceInternal("only support one index for vector field")),
					}, nil
				}
				index := indexes[0]
				indexName = index.IndexInfo.GetIndexName()
				indexID = index.IndexInfo.GetIndexID()
				indexInfos = append(indexInfos, index.IndexInfo)
			}
		}

		info := &querypb.SegmentInfo{
			SegmentID:      segment.ID(),
			SegmentState:   segment.Type(),
			DmChannel:      segment.Shard().VirtualName(),
			PartitionID:    segment.Partition(),
			CollectionID:   segment.Collection(),
			NodeID:         node.GetNodeID(),
			NodeIds:        []int64{node.GetNodeID()},
			MemSize:        segment.MemSize(),
			NumRows:        segment.InsertCount(),
			IndexName:      indexName,
			IndexID:        indexID,
			IndexInfos:     indexInfos,
			StorageVersion: segment.LoadInfo().GetStorageVersion(),
		}
		segmentInfos = append(segmentInfos, info)
	}

	return &querypb.GetSegmentInfoResponse{
		Status: merr.Success(),
		Infos:  segmentInfos,
	}, nil
}

// only used for shard delegator search segments from worker
func (node *QueryNode) SearchSegments(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	channel := req.GetDmlChannels()[0]
	log := log.Ctx(ctx).With(
		zap.Int64("msgID", req.GetReq().GetBase().GetMsgID()),
		zap.Int64("collectionID", req.Req.GetCollectionID()),
		zap.String("channel", channel),
		zap.String("scope", req.GetScope().String()),
	)
	channelsMvcc := make(map[string]uint64)
	for _, ch := range req.GetDmlChannels() {
		channelsMvcc[ch] = req.GetReq().GetMvccTimestamp()
	}
	resp := &internalpb.SearchResults{
		ChannelsMvcc: channelsMvcc,
	}
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	defer node.lifetime.Done()

	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.SearchLabel, metrics.TotalLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
	defer func() {
		if !merr.Ok(resp.GetStatus()) {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.SearchLabel, metrics.FailLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
		}
	}()

	log.Debug("start to search segments on worker",
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
	)
	searchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := timerecord.NewTimeRecorder("searchSegments")
	log.Debug("search segments...")

	if !node.manager.Collection.Ref(req.Req.GetCollectionID(), 1) {
		err := merr.WrapErrCollectionNotLoaded(req.GetReq().GetCollectionID())
		log.Warn("failed to search segments", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	collection := node.manager.Collection.Get(req.Req.GetCollectionID())
	defer func() {
		node.manager.Collection.Unref(req.GetReq().GetCollectionID(), 1)
	}()

	var task scheduler.Task
	if paramtable.Get().QueryNodeCfg.UseStreamComputing.GetAsBool() {
		task = tasks.NewStreamingSearchTask(searchCtx, collection, node.manager, req, node.serverID)
	} else {
		task = tasks.NewSearchTask(searchCtx, collection, node.manager, req, node.serverID)
	}

	if err := node.scheduler.Add(task); err != nil {
		log.Warn("failed to search channel", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	err := task.Wait()
	if err != nil {
		log.Warn("failed to search segments", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("search segments done, channel = %s, segmentIDs = %v",
		channel,
		req.GetSegmentIDs(),
	))

	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.SearchLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.SearchLabel, metrics.SuccessLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()

	resp = task.SearchResult()
	resp.GetCostAggregation().ResponseTime = tr.ElapseSpan().Milliseconds()
	resp.GetCostAggregation().TotalNQ = node.scheduler.GetWaitingTaskTotalNQ()
	if req.GetReq().GetIsTopkReduce() {
		resp.IsTopkReduce = true
	}
	resp.IsRecallEvaluation = req.GetReq().GetIsRecallEvaluation()
	return resp, nil
}

// Search performs replica search tasks.
func (node *QueryNode) Search(ctx context.Context, req *querypb.SearchRequest) (*internalpb.SearchResults, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.Strings("channels", req.GetDmlChannels()),
		zap.Int64("nq", req.GetReq().GetNq()),
	)

	log.Debug("Received SearchRequest",
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("mvccTimestamp", req.GetReq().GetMvccTimestamp()))

	tr := timerecord.NewTimeRecorderWithTrace(ctx, "SearchRequest")

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &internalpb.SearchResults{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	resp := &internalpb.SearchResults{
		Status: merr.Success(),
	}
	collection := node.manager.Collection.Get(req.GetReq().GetCollectionID())
	if collection == nil {
		resp.Status = merr.Status(merr.WrapErrCollectionNotLoaded(req.GetReq().GetCollectionID()))
		return resp, nil
	}

	if len(req.GetDmlChannels()) != 1 {
		err := merr.WrapErrParameterInvalid(1, len(req.GetDmlChannels()), "count of channel to be searched should only be 1, wrong code")
		resp.Status = merr.Status(err)
		log.Warn("got wrong number of channels to be searched", zap.Error(err))
		return resp, nil
	}

	ch := req.GetDmlChannels()[0]
	channelReq := &querypb.SearchRequest{
		Req:             req.Req,
		DmlChannels:     []string{ch},
		SegmentIDs:      req.SegmentIDs,
		Scope:           req.Scope,
		TotalChannelNum: req.TotalChannelNum,
	}
	ret, err := node.searchChannel(ctx, channelReq, ch)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	tr.RecordSpan()
	ret.Status = merr.Success()

	reduceLatency := tr.RecordSpan()
	metrics.QueryNodeReduceLatency.
		WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.SearchLabel, metrics.ReduceShards, metrics.BatchReduce).
		Observe(float64(reduceLatency.Milliseconds()))
	metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(node.GetNodeID(), 10), metrics.SearchLabel).
		Add(float64(proto.Size(req)))

	if ret.GetCostAggregation() != nil {
		ret.GetCostAggregation().ResponseTime = tr.ElapseSpan().Milliseconds()
	}
	return ret, nil
}

// only used for delegator query segments from worker
func (node *QueryNode) QuerySegments(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	resp := &internalpb.RetrieveResults{
		Status: merr.Success(),
	}
	msgID := req.Req.Base.GetMsgID()
	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID()
	channel := req.GetDmlChannels()[0]
	log := log.Ctx(ctx).With(
		zap.Int64("msgID", msgID),
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.String("channel", channel),
		zap.String("scope", req.GetScope().String()),
	)

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	defer node.lifetime.Done()

	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.QueryLabel, metrics.TotalLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
	defer func() {
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.QueryLabel, metrics.FailLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
		}
	}()

	log.Debug("start do query segments", zap.Int64s("segmentIDs", req.GetSegmentIDs()))
	// add cancel when error occurs
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tr := timerecord.NewTimeRecorder("querySegments")
	if !node.manager.Collection.Ref(req.Req.GetCollectionID(), 1) {
		err := merr.WrapErrCollectionNotLoaded(req.GetReq().GetCollectionID())
		log.Warn("failed to query segments", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	collection := node.manager.Collection.Get(req.Req.GetCollectionID())
	defer func() {
		node.manager.Collection.Unref(req.GetReq().GetCollectionID(), 1)
	}()
	// Send task to scheduler and wait until it finished.
	task := tasks.NewQueryTask(queryCtx, collection, node.manager, req)
	if err := node.scheduler.Add(task); err != nil {
		log.Warn("failed to add query task into scheduler", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}
	err := task.Wait()
	if err != nil {
		log.Warn("failed to query channel", zap.Error(err))
		resp.Status = merr.Status(err)
		return resp, nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("do query done, traceID = %s,  vChannel = %s, segmentIDs = %v",
		traceID,
		channel,
		req.GetSegmentIDs(),
	))

	// TODO QueryNodeSQLatencyInQueue QueryNodeReduceLatency
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.QueryLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.QueryLabel, metrics.SuccessLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
	result := task.Result()
	result.GetCostAggregation().ResponseTime = latency.Milliseconds()
	result.GetCostAggregation().TotalNQ = node.scheduler.GetWaitingTaskTotalNQ()
	return result, nil
}

// Query performs replica query tasks.
func (node *QueryNode) Query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.Strings("shards", req.GetDmlChannels()),
	)

	log.Debug("received query request",
		zap.Int64s("outputFields", req.GetReq().GetOutputFieldsId()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("mvccTimestamp", req.GetReq().GetMvccTimestamp()),
		zap.Bool("isCount", req.GetReq().GetIsCount()),
	)
	tr := timerecord.NewTimeRecorderWithTrace(ctx, "QueryRequest")

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &internalpb.RetrieveResults{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	toMergeResults := make([]*internalpb.RetrieveResults, len(req.GetDmlChannels()))
	runningGp, runningCtx := errgroup.WithContext(ctx)

	for i, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.QueryRequest{
			Req:         req.Req,
			DmlChannels: []string{ch},
			SegmentIDs:  req.SegmentIDs,
			Scope:       req.Scope,
		}

		idx := i
		runningGp.Go(func() error {
			ret, err := node.queryChannel(runningCtx, req, ch)
			if err == nil {
				err = merr.Error(ret.GetStatus())
			}
			if err != nil {
				return err
			}
			toMergeResults[idx] = ret
			return nil
		})
	}
	if err := runningGp.Wait(); err != nil {
		return &internalpb.RetrieveResults{
			Status: merr.Status(err),
		}, nil
	}

	tr.RecordSpan()
	reducer := segments.CreateInternalReducer(req, node.manager.Collection.Get(req.GetReq().GetCollectionID()).Schema())
	ret, err := reducer.Reduce(ctx, toMergeResults)
	if err != nil {
		return &internalpb.RetrieveResults{
			Status: merr.Status(err),
		}, nil
	}
	reduceLatency := tr.RecordSpan()
	metrics.QueryNodeReduceLatency.WithLabelValues(fmt.Sprint(node.GetNodeID()),
		metrics.QueryLabel, metrics.ReduceShards, metrics.BatchReduce).
		Observe(float64(reduceLatency.Milliseconds()))

	metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(node.GetNodeID(), 10), metrics.QueryLabel).Add(float64(proto.Size(req)))
	relatedDataSize := lo.Reduce(toMergeResults, func(acc int64, result *internalpb.RetrieveResults, _ int) int64 {
		return acc + result.GetCostAggregation().GetTotalRelatedDataSize()
	}, 0)

	if ret.CostAggregation == nil {
		ret.CostAggregation = &internalpb.CostAggregation{}
	}
	ret.CostAggregation.ResponseTime = tr.ElapseSpan().Milliseconds()
	ret.CostAggregation.TotalRelatedDataSize = relatedDataSize
	return ret, nil
}

func (node *QueryNode) QueryStream(req *querypb.QueryRequest, srv querypb.QueryNode_QueryStreamServer) error {
	ctx := srv.Context()
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.Strings("shards", req.GetDmlChannels()),
	)
	concurrentSrv := streamrpc.NewConcurrentQueryStreamServer(srv)

	log.Debug("received query stream request",
		zap.Int64s("outputFields", req.GetReq().GetOutputFieldsId()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.Uint64("guaranteeTimestamp", req.GetReq().GetGuaranteeTimestamp()),
		zap.Uint64("mvccTimestamp", req.GetReq().GetMvccTimestamp()),
		zap.Bool("isCount", req.GetReq().GetIsCount()),
	)

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		concurrentSrv.Send(&internalpb.RetrieveResults{Status: merr.Status(err)})
		return nil
	}
	defer node.lifetime.Done()

	runningGp, runningCtx := errgroup.WithContext(ctx)

	for _, ch := range req.GetDmlChannels() {
		ch := ch
		req := &querypb.QueryRequest{
			Req:         req.Req,
			DmlChannels: []string{ch},
			SegmentIDs:  req.SegmentIDs,
			Scope:       req.Scope,
		}

		runningGp.Go(func() error {
			err := node.queryChannelStream(runningCtx, req, ch, concurrentSrv)
			if err != nil {
				return err
			}
			return nil
		})
	}

	if err := runningGp.Wait(); err != nil {
		concurrentSrv.Send(&internalpb.RetrieveResults{
			Status: merr.Status(err),
		})
		return nil
	}

	metrics.QueryNodeExecuteCounter.WithLabelValues(strconv.FormatInt(node.GetNodeID(), 10), metrics.QueryLabel).Add(float64(proto.Size(req)))
	return nil
}

func (node *QueryNode) QueryStreamSegments(req *querypb.QueryRequest, srv querypb.QueryNode_QueryStreamSegmentsServer) error {
	ctx := srv.Context()
	msgID := req.Req.Base.GetMsgID()
	traceID := trace.SpanFromContext(ctx).SpanContext().TraceID()
	channel := req.GetDmlChannels()[0]
	concurrentSrv := streamrpc.NewConcurrentQueryStreamServer(srv)

	log := log.Ctx(ctx).With(
		zap.Int64("msgID", msgID),
		zap.Int64("collectionID", req.GetReq().GetCollectionID()),
		zap.String("channel", channel),
		zap.String("scope", req.GetScope().String()),
	)

	resp := &internalpb.RetrieveResults{}
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.QueryLabel, metrics.TotalLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
	defer func() {
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.QueryLabel, metrics.FailLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
		}
	}()

	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		resp.Status = merr.Status(err)
		concurrentSrv.Send(resp)
		return nil
	}
	defer node.lifetime.Done()

	log.Debug("start do query with channel", zap.Int64s("segmentIDs", req.GetSegmentIDs()))

	tr := timerecord.NewTimeRecorder("queryChannel")

	err := node.queryStreamSegments(ctx, req, concurrentSrv)
	if err != nil {
		resp.Status = merr.Status(err)
		concurrentSrv.Send(resp)
		return nil
	}

	tr.CtxElapse(ctx, fmt.Sprintf("do query done, traceID = %s,  vChannel = %s, segmentIDs = %v",
		traceID,
		channel,
		req.GetSegmentIDs(),
	))

	// TODO QueryNodeSQLatencyInQueue QueryNodeReduceLatency
	latency := tr.ElapseSpan()
	metrics.QueryNodeSQReqLatency.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.QueryLabel, metrics.FromLeader).Observe(float64(latency.Milliseconds()))
	metrics.QueryNodeSQCount.WithLabelValues(fmt.Sprint(node.GetNodeID()), metrics.QueryLabel, metrics.SuccessLabel, metrics.FromLeader, fmt.Sprint(req.GetReq().GetCollectionID())).Inc()
	return nil
}

// SyncReplicaSegments syncs replica node & segments states
func (node *QueryNode) SyncReplicaSegments(ctx context.Context, req *querypb.SyncReplicaSegmentsRequest) (*commonpb.Status, error) {
	return merr.Success(), nil
}

// ShowConfigurations returns the configurations of queryNode matching req.Pattern
func (node *QueryNode) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		log.Ctx(ctx).Warn("QueryNode.ShowConfigurations failed",
			zap.Int64("nodeId", node.GetNodeID()),
			zap.String("req", req.Pattern),
			zap.Error(err))

		return &internalpb.ShowConfigurationsResponse{
			Status:        merr.Status(err),
			Configuations: nil,
		}, nil
	}
	defer node.lifetime.Done()

	configList := make([]*commonpb.KeyValuePair, 0)
	for key, value := range paramtable.Get().GetComponentConfigurations("querynode", req.Pattern) {
		configList = append(configList,
			&commonpb.KeyValuePair{
				Key:   key,
				Value: value,
			})
	}

	return &internalpb.ShowConfigurationsResponse{
		Status:        merr.Success(),
		Configuations: configList,
	}, nil
}

// GetMetrics return system infos of the query node, such as total memory, memory usage, cpu usage ...
func (node *QueryNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		log.Ctx(ctx).Warn("QueryNode.GetMetrics failed",
			zap.Int64("nodeId", node.GetNodeID()),
			zap.String("req", req.Request),
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status:   merr.Status(err),
			Response: "",
		}, nil
	}
	defer node.lifetime.Done()

	resp := &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		ComponentName: metricsinfo.ConstructComponentName(typeutil.QueryNodeRole, paramtable.GetNodeID()),
	}

	ret, err := node.metricsRequest.ExecuteMetricsRequest(ctx, req)
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}

	resp.Response = ret
	return resp, nil
}

func (node *QueryNode) GetDataDistribution(ctx context.Context, req *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("msgID", req.GetBase().GetMsgID()),
		zap.Int64("nodeID", node.GetNodeID()),
	)
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		log.Warn("QueryNode.GetDataDistribution failed",
			zap.Error(err))

		return &querypb.GetDataDistributionResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	lastModifyTs := node.getDistributionModifyTS()
	distributionChange := func() bool {
		if req.GetLastUpdateTs() == 0 {
			return true
		}

		return req.GetLastUpdateTs() < lastModifyTs
	}

	if !distributionChange() {
		return &querypb.GetDataDistributionResponse{
			Status:       merr.Success(),
			NodeID:       node.GetNodeID(),
			LastModifyTs: lastModifyTs,
		}, nil
	}

	sealedSegments := node.manager.Segment.GetBy(segments.WithType(commonpb.SegmentState_Sealed))
	segmentVersionInfos := make([]*querypb.SegmentVersionInfo, 0, len(sealedSegments))
	for _, s := range sealedSegments {
		segmentVersionInfos = append(segmentVersionInfos, &querypb.SegmentVersionInfo{
			ID:                 s.ID(),
			Collection:         s.Collection(),
			Partition:          s.Partition(),
			Channel:            s.Shard().VirtualName(),
			Version:            s.Version(),
			Level:              s.Level(),
			IsSorted:           s.IsSorted(),
			LastDeltaTimestamp: s.LastDeltaTimestamp(),
			IndexInfo: lo.SliceToMap(s.Indexes(), func(info *segments.IndexedFieldInfo) (int64, *querypb.FieldIndexInfo) {
				return info.IndexInfo.IndexID, info.IndexInfo
			}),
			FieldJsonIndexStats: s.GetFieldJSONIndexStats(),
		})
	}

	channelVersionInfos := make([]*querypb.ChannelVersionInfo, 0)
	leaderViews := make([]*querypb.LeaderView, 0)

	node.delegators.Range(func(key string, delegator delegator.ShardDelegator) bool {
		if !delegator.Serviceable() {
			return true
		}
		channelVersionInfos = append(channelVersionInfos, &querypb.ChannelVersionInfo{
			Channel:    key,
			Collection: delegator.Collection(),
			Version:    delegator.Version(),
		})

		sealed, growing := delegator.GetSegmentInfo(false)
		sealedSegments := make(map[int64]*querypb.SegmentDist)
		for _, item := range sealed {
			for _, segment := range item.Segments {
				sealedSegments[segment.SegmentID] = &querypb.SegmentDist{
					NodeID:  item.NodeID,
					Version: segment.Version,
				}
			}
		}

		numOfGrowingRows := int64(0)
		growingSegments := make(map[int64]*msgpb.MsgPosition)
		for _, entry := range growing {
			segment := node.manager.Segment.GetWithType(entry.SegmentID, segments.SegmentTypeGrowing)
			if segment == nil {
				log.Warn("leader view growing not found", zap.String("channel", key), zap.Int64("segmentID", entry.SegmentID))
				growingSegments[entry.SegmentID] = &msgpb.MsgPosition{}
				continue
			}
			growingSegments[entry.SegmentID] = segment.StartPosition()
			numOfGrowingRows += segment.InsertCount()
		}

		queryView := delegator.GetChannelQueryView()
		leaderViews = append(leaderViews, &querypb.LeaderView{
			Collection:             delegator.Collection(),
			Channel:                key,
			SegmentDist:            sealedSegments,
			GrowingSegments:        growingSegments,
			NumOfGrowingRows:       numOfGrowingRows,
			PartitionStatsVersions: delegator.GetPartitionStatsVersions(ctx),
			TargetVersion:          queryView.GetVersion(),
			Status: &querypb.LeaderViewStatus{
				Serviceable: queryView.Serviceable(),
			},
		})
		return true
	})

	return &querypb.GetDataDistributionResponse{
		Status:          merr.Success(),
		NodeID:          node.GetNodeID(),
		Segments:        segmentVersionInfos,
		Channels:        channelVersionInfos,
		LeaderViews:     leaderViews,
		LastModifyTs:    lastModifyTs,
		MemCapacityInMB: float64(hardware.GetMemoryCount() / 1024 / 1024),
		CpuNum:          int64(hardware.GetCPUNum()),
	}, nil
}

func (node *QueryNode) SyncDistribution(ctx context.Context, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
	defer node.updateDistributionModifyTS()

	log := log.Ctx(ctx).With(zap.Int64("collectionID", req.GetCollectionID()),
		zap.String("channel", req.GetChannel()), zap.Int64("currentNodeID", node.GetNodeID()))
	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	// get shard delegator
	shardDelegator, ok := node.delegators.Get(req.GetChannel())
	if !ok {
		err := merr.WrapErrChannelNotFound(req.GetChannel())
		log.Warn("failed to find shard cluster when sync")
		return merr.Status(err), nil
	}

	// translate segment action
	removeActions := make([]*querypb.SyncAction, 0)
	group, ctx := errgroup.WithContext(ctx)
	for _, action := range req.GetActions() {
		log := log.With(zap.String("Action",
			action.GetType().String()))
		switch action.GetType() {
		case querypb.SyncType_Remove:
			log.Info("sync action", zap.Int64("segmentID", action.SegmentID))
			removeActions = append(removeActions, action)
		case querypb.SyncType_Set:
			log.Info("sync action", zap.Int64("segmentID", action.SegmentID))
			if action.GetInfo() == nil {
				log.Warn("sync request from legacy querycoord without load info, skip")
				continue
			}

			// to pass segment'version, we call load segment one by one
			action := action
			group.Go(func() error {
				return shardDelegator.LoadSegments(ctx, &querypb.LoadSegmentsRequest{
					Base: commonpbutil.NewMsgBase(
						commonpbutil.WithMsgType(commonpb.MsgType_LoadSegments),
						commonpbutil.WithMsgID(req.Base.GetMsgID()),
					),
					Infos:         []*querypb.SegmentLoadInfo{action.GetInfo()},
					Schema:        req.GetSchema(),
					LoadMeta:      req.GetLoadMeta(),
					CollectionID:  req.GetCollectionID(),
					ReplicaID:     req.GetReplicaID(),
					DstNodeID:     action.GetNodeID(),
					Version:       action.GetVersion(),
					NeedTransfer:  false,
					LoadScope:     querypb.LoadScope_Delta,
					IndexInfoList: req.GetIndexInfoList(),
				})
			})
		case querypb.SyncType_UpdateVersion:
			log.Info("sync action",
				zap.Int64("TargetVersion", action.GetTargetVersion()),
				zap.Time("checkPoint", tsoutil.PhysicalTime(action.GetCheckpoint().GetTimestamp())),
				zap.Time("deleteCP", tsoutil.PhysicalTime(action.GetDeleteCP().GetTimestamp())),
				zap.Int64s("partitions", req.GetLoadMeta().GetPartitionIDs()))
			droppedInfos := lo.SliceToMap(action.GetDroppedInTarget(), func(id int64) (int64, uint64) {
				if action.GetCheckpoint() == nil {
					return id, typeutil.MaxTimestamp
				}
				return id, action.GetCheckpoint().Timestamp
			})
			shardDelegator.AddExcludedSegments(droppedInfos)
			flushedInfo := lo.SliceToMap(action.GetSealedInTarget(), func(id int64) (int64, uint64) {
				if action.GetCheckpoint() == nil {
					return id, typeutil.MaxTimestamp
				}
				return id, action.GetCheckpoint().Timestamp
			})
			shardDelegator.AddExcludedSegments(flushedInfo)
			shardDelegator.SyncTargetVersion(action, req.GetLoadMeta().GetPartitionIDs())
		case querypb.SyncType_UpdatePartitionStats:
			log.Info("sync update partition stats versions")
			shardDelegator.SyncPartitionStats(ctx, action.PartitionStatsVersions)
		default:
			return merr.Status(merr.WrapErrServiceInternal("unknown action type", action.GetType().String())), nil
		}
	}

	err := group.Wait()
	if err != nil {
		log.Warn("failed to sync distribution", zap.Error(err))
		return merr.Status(err), nil
	}

	// in case of target node offline, when try to remove segment from leader's distribution, use wildcardNodeID(-1) to skip nodeID check
	for _, action := range removeActions {
		shardDelegator.ReleaseSegments(ctx, &querypb.ReleaseSegmentsRequest{
			NodeID:       -1,
			SegmentIDs:   []int64{action.GetSegmentID()},
			Scope:        querypb.DataScope_Historical,
			CollectionID: req.GetCollectionID(),
		}, true)
	}

	return merr.Success(), nil
}

// Delete is used to forward delete message between delegator and workers.
func (node *QueryNode) Delete(ctx context.Context, req *querypb.DeleteRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionId()),
		zap.String("channel", req.GetVchannelName()),
		zap.Int64("segmentID", req.GetSegmentId()),
		zap.String("scope", req.GetScope().String()),
	)

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return merr.Status(err), nil
	}
	defer node.lifetime.Done()

	log.Debug("QueryNode received worker delete detail", zap.Stringer("info", &deleteRequestStringer{DeleteRequest: req}))

	filters := []segments.SegmentFilter{
		segments.WithID(req.GetSegmentId()),
	}

	// do not add filter for Unknown & All scope, for backward cap
	switch req.GetScope() {
	case querypb.DataScope_Historical:
		filters = append(filters, segments.WithType(segments.SegmentTypeSealed))
	case querypb.DataScope_Streaming:
		filters = append(filters, segments.WithType(segments.SegmentTypeGrowing))
	}

	segments := node.manager.Segment.GetBy(filters...)
	if len(segments) == 0 {
		err := merr.WrapErrSegmentNotFound(req.GetSegmentId())
		log.Warn("segment not found for delete")
		return merr.Status(err), nil
	}

	pks := storage.ParseIDs2PrimaryKeysBatch(req.GetPrimaryKeys())
	var err error
	for _, segment := range segments {
		if req.GetUseLoad() {
			var dd *storage.DeltaData
			dd, err = storage.NewDeltaDataWithData(pks, req.GetTimestamps())
			if err == nil {
				err = segment.LoadDeltaData(ctx, dd)
			}
		} else {
			err = segment.Delete(ctx, pks, req.GetTimestamps())
		}
		if err != nil {
			log.Warn("segment delete failed", zap.Error(err))
			return merr.Status(err), nil
		}
	}

	return merr.Success(), nil
}

// DeleteBatch is the API to apply same delete data into multiple segments.
// it's basically same as `Delete` but cost less memory pressure.
func (node *QueryNode) DeleteBatch(ctx context.Context, req *querypb.DeleteBatchRequest) (*querypb.DeleteBatchResponse, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", req.GetCollectionId()),
		zap.String("channel", req.GetVchannelName()),
		zap.Int64s("segmentIDs", req.GetSegmentIds()),
		zap.String("scope", req.GetScope().String()),
	)

	// check node healthy
	if err := node.lifetime.Add(merr.IsHealthy); err != nil {
		return &querypb.DeleteBatchResponse{
			Status: merr.Status(err),
		}, nil
	}
	defer node.lifetime.Done()

	// log.Debug("QueryNode received worker delete detail", zap.Stringer("info", &deleteRequestStringer{DeleteRequest: req}))

	filters := []segments.SegmentFilter{
		segments.WithIDs(req.GetSegmentIds()...),
	}

	// do not add filter for Unknown & All scope, for backward cap
	switch req.GetScope() {
	case querypb.DataScope_Historical:
		filters = append(filters, segments.WithType(segments.SegmentTypeSealed))
	case querypb.DataScope_Streaming:
		filters = append(filters, segments.WithType(segments.SegmentTypeGrowing))
	}

	segs := node.manager.Segment.GetBy(filters...)

	hitIDs := lo.Map(segs, func(segment segments.Segment, _ int) int64 {
		return segment.ID()
	})
	// calculate missing ids, continue to delete existing ones.
	missingIDs := typeutil.NewSet(req.GetSegmentIds()...).Complement(typeutil.NewSet(hitIDs...))
	if missingIDs.Len() > 0 {
		log.Warn("Delete batch find missing ids", zap.Int64s("missing_ids", missingIDs.Collect()))
	}

	pks := storage.ParseIDs2PrimaryKeysBatch(req.GetPrimaryKeys())

	// control the execution batch parallel with P number
	// maybe it shall be lower in case of heavy CPU usage may impacting search/query
	pool := segments.GetDeletePool()
	futures := make([]*conc.Future[struct{}], 0, len(segs))
	errSet := typeutil.NewConcurrentSet[int64]()

	for _, segment := range segs {
		segment := segment
		futures = append(futures, pool.Submit(func() (struct{}, error) {
			// TODO @silverxia, add interface to use same data struct for segment delete
			// current implementation still copys pks into protobuf(or arrow) struct
			err := segment.Delete(ctx, pks, req.GetTimestamps())
			if err != nil {
				errSet.Insert(segment.ID())
				log.Warn("segment delete failed",
					zap.Int64("segmentID", segment.ID()),
					zap.Error(err))
				return struct{}{}, err
			}
			return struct{}{}, nil
		}))
	}

	// ignore error returned, since error segment is recorded into error set
	_ = conc.AwaitAll(futures...)

	// return merr.Success(), nil
	return &querypb.DeleteBatchResponse{
		Status:    merr.Success(),
		FailedIds: errSet.Collect(),
	}, nil
}

func (node *QueryNode) RunAnalyzer(ctx context.Context, req *querypb.RunAnalyzerRequest) (*milvuspb.RunAnalyzerResponse, error) {
	// get delegator
	sd, ok := node.delegators.Get(req.GetChannel())
	if !ok {
		err := merr.WrapErrChannelNotFound(req.GetChannel())
		log.Warn("RunAnalyzer failed, failed to get shard delegator", zap.Error(err))
		return &milvuspb.RunAnalyzerResponse{
			Status: merr.Status(err),
		}, nil
	}
	// run analyzer
	results, err := sd.RunAnalyzer(ctx, req)
	if err != nil {
		log.Warn("failed to search on delegator", zap.Error(err))
		return &milvuspb.RunAnalyzerResponse{
			Status: merr.Status(err),
		}, nil
	}

	return &milvuspb.RunAnalyzerResponse{
		Status:  merr.Status(nil),
		Results: results,
	}, nil
}

type deleteRequestStringer struct {
	*querypb.DeleteRequest
}

func (req *deleteRequestStringer) String() string {
	var pkInfo string
	switch {
	case req.GetPrimaryKeys().GetIntId() != nil:
		ids := req.GetPrimaryKeys().GetIntId().GetData()
		pkInfo = fmt.Sprintf("Pks range[%d-%d], len: %d", ids[0], ids[len(ids)-1], len(ids))
	case req.GetPrimaryKeys().GetStrId() != nil:
		ids := req.GetPrimaryKeys().GetStrId().GetData()
		pkInfo = fmt.Sprintf("Pks range[%s-%s], len: %d", ids[0], ids[len(ids)-1], len(ids))
	}
	tss := req.GetTimestamps()
	return fmt.Sprintf("%s, timestamp range: [%d-%d]", pkInfo, tss[0], tss[len(tss)-1])
}

func (node *QueryNode) updateDistributionModifyTS() {
	node.lastModifyLock.Lock()
	defer node.lastModifyLock.Unlock()

	node.lastModifyTs = time.Now().UnixNano()
}

func (node *QueryNode) getDistributionModifyTS() int64 {
	node.lastModifyLock.RLock()
	defer node.lastModifyLock.RUnlock()
	return node.lastModifyTs
}
