package wp

import (
	"context"
	"fmt"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/zilliztech/woodpecker/common/config"
	wpMetrics "github.com/zilliztech/woodpecker/common/metrics"
	wpMinioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/woodpecker"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	WALName = "woodpecker"
)

func init() {
	// register the builder to the wal registry.
	registry.RegisterBuilder(&builderImpl{})
	// register the unmarshaler to the message registry.
	message.RegisterMessageIDUnmsarshaler(WALName, UnmarshalMessageID)
}

// builderImpl is the builder for woodpecker opener.
type builderImpl struct{}

// Name of the wal builder, should be a lowercase string.
func (b *builderImpl) Name() string {
	return WALName
}

// Build build a wal instance.
func (b *builderImpl) Build() (walimpls.OpenerImpls, error) {
	cfg, err := b.getWpConfig()
	if err != nil {
		return nil, err
	}
	var minioHandler wpMinioHandler.MinioHandler
	if cfg.Woodpecker.Storage.IsStorageMinio() {
		minioCli, err := b.getMinioClient(context.TODO())
		if err != nil {
			return nil, err
		}
		minioHandler, err = wpMinioHandler.NewMinioHandlerWithClient(context.Background(), minioCli)
		if err != nil {
			return nil, err
		}
		log.Ctx(context.Background()).Info("create minio handler finish while building wp opener")
	}
	etcdCli, err := b.getEtcdClient(context.TODO())
	if err != nil {
		return nil, err
	}
	log.Ctx(context.Background()).Info("create etcd client finish while building wp opener")
	var wpClient woodpecker.Client
	if cfg.Woodpecker.Storage.IsStorageService() {
		wpClient, err = woodpecker.NewClient(context.Background(), cfg, etcdCli, true)
	} else {
		wpClient, err = woodpecker.NewEmbedClient(context.Background(), cfg, etcdCli, minioHandler, true)
	}
	if err != nil {
		return nil, err
	}
	log.Ctx(context.Background()).Info("build wp opener finish", zap.String("wpClientInstance", fmt.Sprintf("%p", wpClient)))
	wpMetrics.RegisterWoodpeckerWithRegisterer(metrics.GetRegisterer())
	return &openerImpl{
		c: wpClient,
	}, nil
}

func (b *builderImpl) getWpConfig() (*config.Configuration, error) {
	wpConfig, err := config.NewConfiguration()
	if err != nil {
		return nil, err
	}
	err = b.setCustomWpConfig(wpConfig, &paramtable.Get().WoodpeckerCfg)
	if err != nil {
		return nil, err
	}
	return wpConfig, nil
}

func (b *builderImpl) setCustomWpConfig(wpConfig *config.Configuration, cfg *paramtable.WoodpeckerConfig) error {
	// set the rootPath as the prefix for wp object storage
	wpConfig.Woodpecker.Meta.Prefix = fmt.Sprintf("%s/wp", paramtable.Get().EtcdCfg.RootPath.GetValue())
	// logClient
	wpConfig.Woodpecker.Client.Auditor.MaxInterval = int(cfg.AuditorMaxInterval.GetAsDurationByParse().Seconds())
	wpConfig.Woodpecker.Client.SegmentAppend.MaxRetries = cfg.AppendMaxRetries.GetAsInt()
	wpConfig.Woodpecker.Client.SegmentAppend.QueueSize = cfg.AppendQueueSize.GetAsInt()
	wpConfig.Woodpecker.Client.SegmentRollingPolicy.MaxSize = cfg.SegmentRollingMaxSize.GetAsSize()
	wpConfig.Woodpecker.Client.SegmentRollingPolicy.MaxInterval = int(cfg.SegmentRollingMaxTime.GetAsDurationByParse().Seconds())
	wpConfig.Woodpecker.Client.SegmentRollingPolicy.MaxBlocks = cfg.SegmentRollingMaxBlocks.GetAsInt64()

	// quorum configuration
	b.setQuorumConfig(wpConfig, cfg)

	// logStore
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval = int(cfg.SyncMaxInterval.GetAsDurationByParse().Milliseconds())
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage = int(cfg.SyncMaxIntervalForLocalStorage.GetAsDurationByParse().Milliseconds())
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxEntries = cfg.SyncMaxEntries.GetAsInt()
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes = cfg.SyncMaxBytes.GetAsSize()
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushRetries = cfg.FlushMaxRetries.GetAsInt()
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = cfg.FlushMaxSize.GetAsSize()
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads = cfg.FlushMaxThreads.GetAsInt()
	wpConfig.Woodpecker.Logstore.SegmentSyncPolicy.RetryInterval = int(cfg.RetryInterval.GetAsDurationByParse().Milliseconds())
	wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxBytes = cfg.CompactionSize.GetAsSize()
	wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelUploads = cfg.CompactionMaxParallelUploads.GetAsInt()
	wpConfig.Woodpecker.Logstore.SegmentCompactionPolicy.MaxParallelReads = cfg.CompactionMaxParallelReads.GetAsInt()
	wpConfig.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize = cfg.ReaderMaxBatchSize.GetAsSize()
	wpConfig.Woodpecker.Logstore.SegmentReadPolicy.MaxFetchThreads = cfg.ReaderMaxFetchThreads.GetAsInt()
	// storage
	wpConfig.Woodpecker.Storage.Type = cfg.StorageType.GetValue()
	wpConfig.Woodpecker.Storage.RootPath = cfg.RootPath.GetValue()

	// set bucketName
	wpConfig.Minio.BucketName = paramtable.Get().MinioCfg.BucketName.GetValue()
	wpConfig.Minio.RootPath = fmt.Sprintf("%s/wp", paramtable.Get().MinioCfg.RootPath.GetValue())

	// set log
	wpConfig.Log.Level = paramtable.Get().LogCfg.Level.GetValue()
	wpConfig.Log.Format = paramtable.Get().LogCfg.Format.GetValue()
	wpConfig.Log.Stdout = paramtable.Get().LogCfg.Stdout.GetAsBool()
	wpConfig.Log.File.RootPath = paramtable.Get().LogCfg.RootPath.GetValue()
	wpConfig.Log.File.MaxSize = paramtable.Get().LogCfg.MaxSize.GetAsInt()
	wpConfig.Log.File.MaxAge = paramtable.Get().LogCfg.MaxAge.GetAsInt()
	wpConfig.Log.File.MaxBackups = paramtable.Get().LogCfg.MaxBackups.GetAsInt()

	return nil
}

func (b *builderImpl) setQuorumConfig(wpConfig *config.Configuration, cfg *paramtable.WoodpeckerConfig) {
	// Buffer pools for different regions - array of QuorumBufferPool
	var bufferPools []config.QuorumBufferPool

	if region1Seeds := cfg.QuorumBufferPoolsRegion1.GetValue(); region1Seeds != "" {
		bufferPools = append(bufferPools, config.QuorumBufferPool{
			Name:  "region1",
			Seeds: parseSeeds(region1Seeds),
		})
	}

	if region2Seeds := cfg.QuorumBufferPoolsRegion2.GetValue(); region2Seeds != "" {
		bufferPools = append(bufferPools, config.QuorumBufferPool{
			Name:  "region2",
			Seeds: parseSeeds(region2Seeds),
		})
	}

	if region3Seeds := cfg.QuorumBufferPoolsRegion3.GetValue(); region3Seeds != "" {
		bufferPools = append(bufferPools, config.QuorumBufferPool{
			Name:  "region3",
			Seeds: parseSeeds(region3Seeds),
		})
	}

	wpConfig.Woodpecker.Client.Quorum.BufferPools = bufferPools

	// Quorum selection strategy
	wpConfig.Woodpecker.Client.Quorum.SelectStrategy.AffinityMode = cfg.QuorumAffinityMode.GetValue()
	wpConfig.Woodpecker.Client.Quorum.SelectStrategy.Replicas = cfg.QuorumReplicas.GetAsInt()
	wpConfig.Woodpecker.Client.Quorum.SelectStrategy.Strategy = cfg.QuorumStrategy.GetValue()

	// Custom placement for replicas - array of CustomPlacement
	var customPlacements []config.CustomPlacement

	// Set replica 1 configuration
	if cfg.QuorumCustomPlacementReplica1Region.GetValue() != "" ||
		cfg.QuorumCustomPlacementReplica1AZ.GetValue() != "" ||
		cfg.QuorumCustomPlacementReplica1RG.GetValue() != "" {
		customPlacements = append(customPlacements, config.CustomPlacement{
			Name:          "replica1",
			Region:        cfg.QuorumCustomPlacementReplica1Region.GetValue(),
			Az:            cfg.QuorumCustomPlacementReplica1AZ.GetValue(),
			ResourceGroup: cfg.QuorumCustomPlacementReplica1RG.GetValue(),
		})
	}

	// Set replica 2 configuration
	if cfg.QuorumCustomPlacementReplica2Region.GetValue() != "" ||
		cfg.QuorumCustomPlacementReplica2AZ.GetValue() != "" ||
		cfg.QuorumCustomPlacementReplica2RG.GetValue() != "" {
		customPlacements = append(customPlacements, config.CustomPlacement{
			Name:          "replica2",
			Region:        cfg.QuorumCustomPlacementReplica2Region.GetValue(),
			Az:            cfg.QuorumCustomPlacementReplica2AZ.GetValue(),
			ResourceGroup: cfg.QuorumCustomPlacementReplica2RG.GetValue(),
		})
	}

	// Set replica 3 configuration
	if cfg.QuorumCustomPlacementReplica3Region.GetValue() != "" ||
		cfg.QuorumCustomPlacementReplica3AZ.GetValue() != "" ||
		cfg.QuorumCustomPlacementReplica3RG.GetValue() != "" {
		customPlacements = append(customPlacements, config.CustomPlacement{
			Name:          "replica3",
			Region:        cfg.QuorumCustomPlacementReplica3Region.GetValue(),
			Az:            cfg.QuorumCustomPlacementReplica3AZ.GetValue(),
			ResourceGroup: cfg.QuorumCustomPlacementReplica3RG.GetValue(),
		})
	}

	// Set replica 4 configuration
	if cfg.QuorumCustomPlacementReplica4Region.GetValue() != "" ||
		cfg.QuorumCustomPlacementReplica4AZ.GetValue() != "" ||
		cfg.QuorumCustomPlacementReplica4RG.GetValue() != "" {
		customPlacements = append(customPlacements, config.CustomPlacement{
			Name:          "replica4",
			Region:        cfg.QuorumCustomPlacementReplica4Region.GetValue(),
			Az:            cfg.QuorumCustomPlacementReplica4AZ.GetValue(),
			ResourceGroup: cfg.QuorumCustomPlacementReplica4RG.GetValue(),
		})
	}

	// Set replica 5 configuration
	if cfg.QuorumCustomPlacementReplica5Region.GetValue() != "" ||
		cfg.QuorumCustomPlacementReplica5AZ.GetValue() != "" ||
		cfg.QuorumCustomPlacementReplica5RG.GetValue() != "" {
		customPlacements = append(customPlacements, config.CustomPlacement{
			Name:          "replica5",
			Region:        cfg.QuorumCustomPlacementReplica5Region.GetValue(),
			Az:            cfg.QuorumCustomPlacementReplica5AZ.GetValue(),
			ResourceGroup: cfg.QuorumCustomPlacementReplica5RG.GetValue(),
		})
	}

	wpConfig.Woodpecker.Client.Quorum.SelectStrategy.CustomPlacement = customPlacements
}

// parseSeeds parses comma-separated seed addresses into a slice
func parseSeeds(seeds string) []string {
	if seeds == "" {
		return nil
	}
	var result []string
	for _, seed := range strings.Split(seeds, ",") {
		if trimmed := strings.TrimSpace(seed); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func (b *builderImpl) getMinioClient(ctx context.Context) (*minio.Client, error) {
	c := objectstorage.NewDefaultConfig()
	params := paramtable.Get()
	opts := []objectstorage.Option{
		objectstorage.RootPath(params.MinioCfg.RootPath.GetValue()),
		objectstorage.Address(params.MinioCfg.Address.GetValue()),
		objectstorage.AccessKeyID(params.MinioCfg.AccessKeyID.GetValue()),
		objectstorage.SecretAccessKeyID(params.MinioCfg.SecretAccessKey.GetValue()),
		objectstorage.UseSSL(params.MinioCfg.UseSSL.GetAsBool()),
		objectstorage.SslCACert(params.MinioCfg.SslCACert.GetValue()),
		objectstorage.BucketName(params.MinioCfg.BucketName.GetValue()),
		objectstorage.UseIAM(params.MinioCfg.UseIAM.GetAsBool()),
		objectstorage.CloudProvider(params.MinioCfg.CloudProvider.GetValue()),
		objectstorage.IAMEndpoint(params.MinioCfg.IAMEndpoint.GetValue()),
		objectstorage.UseVirtualHost(params.MinioCfg.UseVirtualHost.GetAsBool()),
		objectstorage.Region(params.MinioCfg.Region.GetValue()),
		objectstorage.RequestTimeout(params.MinioCfg.RequestTimeoutMs.GetAsInt64()),
		objectstorage.CreateBucket(true),
		objectstorage.GcpCredentialJSON(params.MinioCfg.GcpCredentialJSON.GetValue()),
	}
	for _, opt := range opts {
		opt(c)
	}
	return objectstorage.NewMinioClient(ctx, c)
}

func (b *builderImpl) getEtcdClient(ctx context.Context) (*clientv3.Client, error) {
	params := paramtable.Get()
	etcdConfig := &params.EtcdCfg
	log := log.Ctx(ctx)

	etcdCli, err := etcd.CreateEtcdClient(
		etcdConfig.UseEmbedEtcd.GetAsBool(),
		etcdConfig.EtcdEnableAuth.GetAsBool(),
		etcdConfig.EtcdAuthUserName.GetValue(),
		etcdConfig.EtcdAuthPassword.GetValue(),
		etcdConfig.EtcdUseSSL.GetAsBool(),
		etcdConfig.Endpoints.GetAsStrings(),
		etcdConfig.EtcdTLSCert.GetValue(),
		etcdConfig.EtcdTLSKey.GetValue(),
		etcdConfig.EtcdTLSCACert.GetValue(),
		etcdConfig.EtcdTLSMinVersion.GetValue())
	if err != nil {
		log.Warn("Woodpecker create connection to etcd failed", zap.Error(err))
		return nil, err
	}
	return etcdCli, nil
}
