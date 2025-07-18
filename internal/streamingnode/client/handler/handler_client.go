package handler

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/assignment"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	streamingserviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/tracer"
	"github.com/milvus-io/milvus/pkg/v2/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	_                           HandlerClient = (*handlerClientImpl)(nil)
	ErrClientClosed                           = errors.New("handler client is closed")
	ErrClientAssignmentNotReady               = errors.New("handler client assignment not ready")
	ErrReadOnlyWAL                            = errors.New("wal is read only")
)

type (
	Producer = producer.Producer
	Consumer = consumer.Consumer
)

// ProducerOptions is the options for creating a producer.
type ProducerOptions struct {
	// PChannel is the pchannel of the producer.
	PChannel string
}

// ConsumerOptions is the options for creating a consumer.
type ConsumerOptions struct {
	// PChannel is the pchannel of the consumer.
	PChannel string

	// VChannel is the vchannel of the consumer.
	VChannel string

	// DeliverPolicy is the deliver policy of the consumer.
	DeliverPolicy options.DeliverPolicy

	// DeliverFilters is the deliver filters of the consumer.
	DeliverFilters []options.DeliverFilter

	// Handler is the message handler used to handle message after recv from consumer.
	MessageHandler message.Handler
}

// HandlerClient is the interface that wraps streamingpb.StreamingNodeHandlerServiceClient.
// HandlerClient wraps the PChannel Assignment Service Discovery.
// Provides the ability to create pchannel-level producer and consumer.
type HandlerClient interface {
	// GetLatestMVCCTimestampIfLocal gets the latest mvcc timestamp of the vchannel.
	// If the wal is located at remote, it will return 0, error.
	GetLatestMVCCTimestampIfLocal(ctx context.Context, vchannel string) (uint64, error)

	// GetWALMetricsIfLocal gets the metrics of the local wal.
	// It will only return the metrics of the local wal but not the remote wal.
	GetWALMetricsIfLocal(ctx context.Context) (*types.StreamingNodeMetrics, error)

	// CreateProducer creates a producer.
	// Producer is a stream client without keep alive promise.
	// It will be available until context canceled, active close, streaming error or remote server wal closing.
	// Because of there's no more ProducerOptions except PChannel, so a producer of same PChannel is shared by reference count.
	CreateProducer(ctx context.Context, opts *ProducerOptions) (Producer, error)

	// CreateConsumer creates a consumer.
	// Consumer is a stream client without keep alive promise.
	// It will be available until context canceled, active close, streaming error or remote server wal closing.
	// A consumer will not share stream connection with other consumers.
	CreateConsumer(ctx context.Context, opts *ConsumerOptions) (Consumer, error)

	// Close closes the handler client.
	// It will only stop the underlying service discovery, but don't stop the producer and consumer created by it.
	// So please close Producer and Consumer created by it before close the handler client.
	Close()
}

// NewHandlerClient creates a new handler client.
func NewHandlerClient(w types.AssignmentDiscoverWatcher) HandlerClient {
	rb := resolver.NewChannelAssignmentBuilder(w)
	dialTimeout := paramtable.Get().StreamingNodeGrpcClientCfg.DialTimeout.GetAsDuration(time.Millisecond)
	dialOptions := getDialOptions(rb)
	conn := lazygrpc.NewConn(func(ctx context.Context) (*grpc.ClientConn, error) {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return grpc.DialContext(
			ctx,
			resolver.ChannelAssignmentResolverScheme+":///"+typeutil.StreamingNodeRole,
			dialOptions..., // TODO: we should use dynamic service config in future by add it to resolver.
		)
	})
	watcher := assignment.NewWatcher(rb.Resolver())
	return &handlerClientImpl{
		lifetime:         typeutil.NewLifetime(),
		service:          lazygrpc.WithServiceCreator(conn, streamingpb.NewStreamingNodeHandlerServiceClient),
		rb:               rb,
		watcher:          watcher,
		rebalanceTrigger: w,
		newProducer:      producer.CreateProducer,
		newConsumer:      consumer.CreateConsumer,
	}
}

// getDialOptions returns grpc dial options.
func getDialOptions(rb resolver.Builder) []grpc.DialOption {
	cfg := &paramtable.Get().StreamingNodeGrpcClientCfg
	tlsCfg := &paramtable.Get().InternalTLSCfg
	retryPolicy := cfg.GetDefaultRetryPolicy()
	retryPolicy["retryableStatusCodes"] = []string{"UNAVAILABLE"}
	defaultServiceConfig := map[string]interface{}{
		"loadBalancingConfig": []map[string]interface{}{
			{picker.ServerIDPickerBalancerName: map[string]interface{}{}},
		},
		"methodConfig": []map[string]interface{}{
			{
				"name": []map[string]string{
					{"service": "milvus.proto.streaming.StreamingNodeHandlerService"},
				},
				"waitForReady": true,
				"retryPolicy":  retryPolicy,
			},
		},
	}
	defaultServiceConfigJSON, err := json.Marshal(defaultServiceConfig)
	if err != nil {
		panic(err)
	}
	creds, err := tlsCfg.GetClientCreds(context.Background())
	if err != nil {
		panic(err)
	}
	dialOptions := cfg.GetDialOptionsFromConfig()
	dialOptions = append(dialOptions,
		grpc.WithBlock(),
		grpc.WithResolvers(rb),
		grpc.WithTransportCredentials(creds),
		grpc.WithChainUnaryInterceptor(
			otelgrpc.UnaryClientInterceptor(tracer.GetInterceptorOpts()...),
			interceptor.ClusterInjectionUnaryClientInterceptor(),
			streamingserviceinterceptor.NewStreamingServiceUnaryClientInterceptor(),
		),
		grpc.WithChainStreamInterceptor(
			otelgrpc.StreamClientInterceptor(tracer.GetInterceptorOpts()...),
			interceptor.ClusterInjectionStreamClientInterceptor(),
			streamingserviceinterceptor.NewStreamingServiceStreamClientInterceptor(),
		),
		grpc.WithReturnConnectionError(),
		grpc.WithDefaultServiceConfig(string(defaultServiceConfigJSON)),
	)
	return dialOptions
}
