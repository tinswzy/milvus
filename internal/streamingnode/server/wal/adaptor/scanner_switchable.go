package adaptor

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchantempstore"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	_ switchableScanner = (*tailingScanner)(nil)
	_ switchableScanner = (*catchupScanner)(nil)
)

// newSwitchableScanner creates a new switchable scanner.
func newSwithableScanner(
	scannerName string,
	logger *log.MLogger,
	innerWAL walimpls.ROWALImpls,
	writeAheadBuffer wab.ROWriteAheadBuffer,
	deliverPolicy options.DeliverPolicy,
	msgChan chan<- message.ImmutableMessage,
) switchableScanner {
	return &catchupScanner{
		switchableScannerImpl: switchableScannerImpl{
			scannerName:      scannerName,
			logger:           logger,
			innerWAL:         innerWAL,
			msgChan:          msgChan,
			writeAheadBuffer: writeAheadBuffer,
		},
		deliverPolicy:          deliverPolicy,
		exclusiveStartTimeTick: 0,
	}
}

// switchableScanner is a scanner that can switch between Catchup and Tailing mode
type switchableScanner interface {
	// Execute make a scanner work at background.
	// When the scanner want to change the mode, it will return a new scanner with the new mode.
	// When error is returned, the scanner is canceled and unrecoverable forever.
	Do(ctx context.Context) (switchableScanner, error)
}

type switchableScannerImpl struct {
	scannerName      string
	logger           *log.MLogger
	innerWAL         walimpls.ROWALImpls
	msgChan          chan<- message.ImmutableMessage
	writeAheadBuffer wab.ROWriteAheadBuffer
}

func (s *switchableScannerImpl) HandleMessage(ctx context.Context, msg message.ImmutableMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.msgChan <- msg:
		return nil
	}
}

// catchupScanner is a scanner that make a read at underlying wal, and try to catchup the writeahead buffer then switch to tailing mode.
type catchupScanner struct {
	switchableScannerImpl
	deliverPolicy                       options.DeliverPolicy
	exclusiveStartTimeTick              uint64 // scanner should filter out the message that less than or equal to this time tick.
	lastConfirmedMessageIDForOldVersion message.MessageID
}

func (s *catchupScanner) Do(ctx context.Context) (switchableScanner, error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		scanner, err := s.createReaderWithBackoff(ctx, s.deliverPolicy)
		if err != nil {
			// Only the cancellation error will be returned, other error will keep backoff.
			return nil, err
		}
		switchedScanner, err := s.consumeWithScanner(ctx, scanner)
		if err != nil {
			s.logger.Warn("scanner consuming was interrpurted with error, start a backoff", zap.Error(err))
			continue
		}
		return switchedScanner, nil
	}
}

func (s *catchupScanner) consumeWithScanner(ctx context.Context, scanner walimpls.ScannerImpls) (switchableScanner, error) {
	defer scanner.Close()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-scanner.Chan():
			if !ok {
				return nil, scanner.Error()
			}

			if msg.Version() == message.VersionOld {
				if s.lastConfirmedMessageIDForOldVersion == nil {
					s.logger.Info(
						"scanner find a old version message, set it as the last confirmed message id for all old version message",
						zap.Stringer("messageID", msg.MessageID()),
					)
					s.lastConfirmedMessageIDForOldVersion = msg.MessageID()
				}
				// We always use first consumed message as the last confirmed message id for old version message.
				// After upgrading from old milvus:
				// The wal will be read at consuming side as following:
				// msgv0, msgv0 ..., msgv0, msgv1, msgv1, msgv1, ...
				// the msgv1 will be read after all msgv0 is consumed as soon as possible.
				// so the last confirm is set to the first msgv0 message for all old version message is ok.
				var err error
				messageID := msg.MessageID()
				msg, err = newOldVersionImmutableMessage(ctx, s.innerWAL.Channel().Name, s.lastConfirmedMessageIDForOldVersion, msg)
				if errors.Is(err, vchantempstore.ErrNotFound) {
					// Skip the message's vchannel is not found in the vchannel temp store.
					s.logger.Info("skip the old version message because vchannel not found", zap.Stringer("messageID", messageID))
					continue
				}
				if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
					return nil, err
				}
				if err != nil {
					panic("unrechable: unexpected error found: " + err.Error())
				}
			}

			if msg.TimeTick() <= s.exclusiveStartTimeTick {
				// we should filter out the message that less than or equal to this time tick to remove duplicate message
				// when we switch from tailing mode to catchup mode.
				continue
			}
			if err := s.HandleMessage(ctx, msg); err != nil {
				return nil, err
			}
			if msg.MessageType() != message.MessageTypeTimeTick || s.writeAheadBuffer == nil {
				// Only timetick message is keep the same order with the write ahead buffer.
				// So we can only use the timetick message to catchup the write ahead buffer.
				continue
			}
			// Here's a timetick message from the scanner, make tailing read if we catch up the writeahead buffer.
			if reader, err := s.writeAheadBuffer.ReadFromExclusiveTimeTick(ctx, msg.TimeTick()); err == nil {
				s.logger.Info(
					"scanner consuming was interrpted because catup done",
					zap.Uint64("timetick", msg.TimeTick()),
					zap.Stringer("messageID", msg.MessageID()),
					zap.Stringer("lastConfirmedMessageID", msg.LastConfirmedMessageID()),
				)
				return &tailingScanner{
					switchableScannerImpl: s.switchableScannerImpl,
					reader:                reader,
					lastConsumedMessage:   msg,
				}, nil
			}
		}
	}
}

func (s *catchupScanner) createReaderWithBackoff(ctx context.Context, deliverPolicy options.DeliverPolicy) (walimpls.ScannerImpls, error) {
	backoffTimer := typeutil.NewBackoffTimer(typeutil.BackoffTimerConfig{
		Default: 5 * time.Second,
		Backoff: typeutil.BackoffConfig{
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     5 * time.Second,
		},
	})
	backoffTimer.EnableBackoff()
	for {
		bufSize := paramtable.Get().StreamingCfg.WALReadAheadBufferLength.GetAsInt()
		if bufSize < 0 {
			bufSize = 0
		}
		innerScanner, err := s.innerWAL.Read(ctx, walimpls.ReadOption{
			Name:                s.scannerName,
			DeliverPolicy:       deliverPolicy,
			ReadAheadBufferSize: bufSize,
		})
		if err == nil {
			return innerScanner, nil
		}
		if ctx.Err() != nil {
			// The scanner is closing, so stop the backoff.
			return nil, ctx.Err()
		}
		waker, nextInterval := backoffTimer.NextTimer()
		s.logger.Warn("create underlying scanner for wal scanner, start a backoff",
			zap.Duration("nextInterval", nextInterval),
			zap.Error(err),
		)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-waker:
		}
	}
}

// tailingScanner is used to perform a tailing read from the writeaheadbuffer of wal.
type tailingScanner struct {
	switchableScannerImpl
	reader              *wab.WriteAheadBufferReader
	lastConsumedMessage message.ImmutableMessage
}

func (s *tailingScanner) Do(ctx context.Context) (switchableScanner, error) {
	for {
		msg, err := s.reader.Next(ctx)
		if errors.Is(err, wab.ErrEvicted) {
			// The tailing read is failure, switch into catchup mode.
			s.logger.Info(
				"scanner consuming was interrpted because tailing eviction",
				zap.Uint64("timetick", s.lastConsumedMessage.TimeTick()),
				zap.Stringer("messageID", s.lastConsumedMessage.MessageID()),
				zap.Stringer("lastConfirmedMessageID", s.lastConsumedMessage.LastConfirmedMessageID()),
			)
			return &catchupScanner{
				switchableScannerImpl:  s.switchableScannerImpl,
				deliverPolicy:          options.DeliverPolicyStartFrom(s.lastConsumedMessage.LastConfirmedMessageID()),
				exclusiveStartTimeTick: s.lastConsumedMessage.TimeTick(),
			}, nil
		}
		if err != nil {
			return nil, err
		}
		if err := s.HandleMessage(ctx, tailingImmutableMesasge{msg}); err != nil {
			return nil, err
		}
		s.lastConsumedMessage = msg
	}
}

// getScannerModel returns the scanner model.
func getScannerModel(scanner switchableScanner) string {
	if _, ok := scanner.(*tailingScanner); ok {
		return metrics.WALScannerModelTailing
	}
	return metrics.WALScannerModelCatchup
}

type tailingImmutableMesasge struct {
	message.ImmutableMessage
}

// isTailingScanImmutableMessage check whether the message is a tailing message.
func isTailingScanImmutableMessage(msg message.ImmutableMessage) (message.ImmutableMessage, bool) {
	if msg, ok := msg.(tailingImmutableMesasge); ok {
		return msg.ImmutableMessage, true
	}
	return msg, false
}
