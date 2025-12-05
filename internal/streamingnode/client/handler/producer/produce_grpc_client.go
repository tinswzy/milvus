package producer

import (
	"context"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// produceGrpcClient is a wrapped producer server of log messages.
type produceGrpcClient struct {
	streamingpb.StreamingNodeHandlerService_ProduceClient
}

// SendProduceMessage sends the produce message to server.
func (p *produceGrpcClient) SendProduceMessage(requestID int64, msg message.MutableMessage, ctx context.Context) error {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return p.Send(&streamingpb.ProduceRequest{
		Request: &streamingpb.ProduceRequest_Produce{
			Produce: &streamingpb.ProduceMessageRequest{
				RequestId:    requestID,
				Message:      msg.IntoMessageProto(),
				TraceContext: carrier,
			},
		},
	})
}

// InjectToMap 将trace context注入到map中
func InjectToMap(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	if len(carrier) == 0 {
		return nil
	}

	// 转换为map
	result := make(map[string]string)
	for k, v := range carrier {
		result[k] = v
	}

	return result
}

// SendClose sends the close request to server.
func (p *produceGrpcClient) SendClose() error {
	return p.Send(&streamingpb.ProduceRequest{
		Request: &streamingpb.ProduceRequest_Close{
			Close: &streamingpb.CloseProducerRequest{},
		},
	})
}
