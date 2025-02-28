package wp

import (
	"context"
	"os"
	"path"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/woodpecker"

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
	wpClient, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
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
