package wp

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/zilliztech/woodpecker/common/config"
	wpMinioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/woodpecker"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"os"
	"path"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	walName = "woodpecker"
)

func init() {
	// register the builder to the wal registry.
	registry.RegisterBuilder(&builderImpl{})
	// register the unmarshaler to the message registry.
	message.RegisterMessageIDUnmsarshaler(walName, UnmarshalMessageID)
}

// builderImpl is the builder for woodpecker opener.
type builderImpl struct{}

// Name of the wal builder, should be a lowercase string.
func (b *builderImpl) Name() string {
	return walName
}

// Build build a wal instance.
func (b *builderImpl) Build() (walimpls.OpenerImpls, error) {
	cfg, err := b.getWpConfig()
	if err != nil {
		return nil, err
	}
	//wpClient, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	minioCli, err := b.getMinioClient(context.TODO())
	if err != nil {
		return nil, err
	}
	log.Ctx(context.TODO()).Info("create minio client finish while building wp opener")
	minioHandler, err := wpMinioHandler.NewMinioHandlerWithClient(context.Background(), minioCli)
	if err != nil {
		return nil, err
	}
	log.Ctx(context.TODO()).Info("create minio handler finish while building wp opener")
	etcdCli, err := b.getEtcdClient(context.TODO())
	if err != nil {
		return nil, err
	}
	log.Ctx(context.TODO()).Info("create etcd client finish while building wp opener")
	wpClient, err := woodpecker.NewEmbedClient(context.Background(), cfg, etcdCli, minioHandler, true)
	if err != nil {
		return nil, err
	}
	log.Ctx(context.TODO()).Info("build wp opener finish", zap.String("wpClientInstance", fmt.Sprintf("%p", wpClient)))
	return &openerImpl{
		c: wpClient,
	}, nil
}

func (b *builderImpl) getWpConfig() (*config.Configuration, error) {
	cfgDir := paramtable.GetBaseTable().GetConfigDir()
	var defaultYaml = []string{"milvus.yaml", "_test.yaml", "default.yaml", "user.yaml"}
	files := make([]string, 0)
	for _, yaml := range defaultYaml {
		_, err := os.Stat(path.Join(cfgDir, yaml))
		// not found
		if os.IsNotExist(err) {
			continue
		}
		files = append(files, path.Join(cfgDir, yaml))
	}
	log.Ctx(context.Background()).Info("use config files to conn wp", zap.Strings("files", files))
	wpConfig, err := config.NewConfiguration(files...)
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
	// logClient
	wpConfig.Woodpecker.Client.Auditor.MaxInterval = cfg.AuditorMaxInterval.GetAsInt()
	wpConfig.Woodpecker.Client.SegmentAppend.MaxRetries = cfg.AppendMaxRetries.GetAsInt()
	wpConfig.Woodpecker.Client.SegmentAppend.QueueSize = cfg.AppendQueueSize.GetAsInt()
	wpConfig.Woodpecker.Client.SegmentRollingPolicy.MaxSize = cfg.SegmentRollingMaxSize.GetAsInt()
	wpConfig.Woodpecker.Client.SegmentRollingPolicy.MaxInterval = cfg.SegmentRollingMaxTime.GetAsInt()
	// logStore
	wpConfig.Woodpecker.Logstore.LogFileSyncPolicy.MaxInterval = cfg.SyncMaxInterval.GetAsInt()
	wpConfig.Woodpecker.Logstore.LogFileSyncPolicy.MaxEntries = cfg.SyncMaxEntries.GetAsInt()
	wpConfig.Woodpecker.Logstore.LogFileSyncPolicy.MaxBytes = cfg.SyncMaxBytes.GetAsInt()
	wpConfig.Woodpecker.Logstore.LogFileSyncPolicy.MaxFlushRetries = cfg.FlushMaxRetries.GetAsInt()
	wpConfig.Woodpecker.Logstore.LogFileSyncPolicy.MaxFlushSize = cfg.FlushMaxSize.GetAsInt()
	wpConfig.Woodpecker.Logstore.LogFileSyncPolicy.MaxFlushThreads = cfg.FlushMaxThreads.GetAsInt()
	wpConfig.Woodpecker.Logstore.LogFileSyncPolicy.RetryInterval = cfg.RetryInterval.GetAsInt()

	return nil
}

const (
	CloudProviderGCP       = "gcp"
	CloudProviderGCPNative = "gcpnative"
	CloudProviderAWS       = "aws"
	CloudProviderAliyun    = "aliyun"
	CloudProviderAzure     = "azure"
	CloudProviderTencent   = "tencent"
)

// TODO test only
func (b *builderImpl) getMinioClient(ctx context.Context) (*minio.Client, error) {
	minioConfig := paramtable.Get().MinioCfg
	var creds *credentials.Credentials
	newMinioFn := minio.New
	bucketLookupType := minio.BucketLookupAuto

	if minioConfig.UseVirtualHost.GetAsBool() {
		bucketLookupType = minio.BucketLookupDNS
	}

	matchedDefault := false
	switch minioConfig.CloudProvider.GetValue() {
	case CloudProviderAliyun:
		panic("not support aliyun for test")
	case CloudProviderGCP:
		panic("not support aliyun for test")
	case CloudProviderTencent:
		panic("not support aliyun for test")
	default: // aws, minio
		matchedDefault = true
	}

	if matchedDefault {
		// aws, minio
		if minioConfig.UseIAM.GetAsBool() {
			creds = credentials.NewIAM("")
		} else {
			creds = credentials.NewStaticV4(minioConfig.AccessKeyID.GetValue(), minioConfig.SecretAccessKey.GetValue(), "")
		}
	}

	// We must set the cert path by os environment variable "SSL_CERT_FILE",
	// because the minio.DefaultTransport() need this path to read the file content,
	// we shouldn't read this file by ourself.
	if minioConfig.UseSSL.GetAsBool() && len(minioConfig.SslCACert.GetValue()) > 0 {
		err := os.Setenv("SSL_CERT_FILE", minioConfig.SslCACert.GetValue())
		if err != nil {
			return nil, err
		}
	}

	minioOpts := &minio.Options{
		BucketLookup: bucketLookupType,
		Creds:        creds,
		Secure:       minioConfig.UseSSL.GetAsBool(),
		Region:       minioConfig.Region.GetValue(),
	}
	minIOClient, err := newMinioFn(minioConfig.Address.GetValue(), minioOpts)
	// options nil or invalid formatted endpoint, don't need to retry
	if err != nil {
		return nil, err
	}
	var bucketExists bool
	// check valid in first query
	checkBucketFn := func() error {
		bucketExists, err = minIOClient.BucketExists(ctx, minioConfig.BucketName.GetValue())
		if err != nil {
			log.Warn("failed to check blob bucket exist when build woodpecker walimpls instance", zap.String("bucket", minioConfig.BucketName.GetValue()), zap.Error(err))
			return err
		}
		if !bucketExists {
			// always create bucket if not exists
			//if true {
			log.Info("blob bucket not exist, create bucket when build woodpecker walimpls instance.", zap.String("bucket name", minioConfig.BucketName.GetValue()))
			err := minIOClient.MakeBucket(ctx, minioConfig.BucketName.GetValue(), minio.MakeBucketOptions{})
			if err != nil {
				log.Warn("failed to create blob bucket when build woodpecker walimpls instance", zap.String("bucket", minioConfig.BucketName.GetValue()), zap.Error(err))
				return err
			}
			//} else {
			//	return fmt.Errorf("bucket %s not Existed", minioConfig.BucketName.GetValue())
			//}
		}
		return nil
	}
	// one time for test only
	err = checkBucketFn()
	if err != nil {
		return nil, err
	}
	return minIOClient, nil
}

// TODO test only
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
		log.Warn("Woodpecker walimpls connect to etcd failed", zap.Error(err))
		return nil, err
	}
	return etcdCli, nil
}
