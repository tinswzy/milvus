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

package paramtable

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestServiceParam(t *testing.T) {
	var SParams ServiceParam
	bt := NewBaseTable(SkipRemote(true))
	SParams.init(bt)

	t.Run("test MQConfig", func(t *testing.T) {
		Params := &SParams.MQCfg
		assert.Equal(t, 100*time.Millisecond, Params.CheckInterval.GetAsDuration(time.Second))
		assert.Equal(t, 16, Params.TargetBufSize.GetAsInt())
		assert.Equal(t, 3*time.Second, Params.MaxTolerantLag.GetAsDuration(time.Second))
		assert.Equal(t, 60*time.Minute, Params.MaxPositionTsGap.GetAsDuration(time.Minute))
	})

	t.Run("test etcdConfig", func(t *testing.T) {
		Params := &SParams.EtcdCfg

		assert.NotZero(t, len(Params.Endpoints.GetAsStrings()))
		t.Logf("etcd endpoints = %s", Params.Endpoints.GetAsStrings())

		assert.NotEqual(t, Params.MetaRootPath, "")
		t.Logf("meta root path = %s", Params.MetaRootPath.GetValue())

		assert.NotEqual(t, Params.KvRootPath, "")
		t.Logf("kv root path = %s", Params.KvRootPath.GetValue())

		assert.NotNil(t, Params.EtcdUseSSL.GetAsBool())
		t.Logf("use ssl = %t", Params.EtcdUseSSL.GetAsBool())

		assert.NotEmpty(t, Params.EtcdTLSKey.GetValue())
		t.Logf("tls key = %s", Params.EtcdTLSKey.GetValue())

		assert.NotEmpty(t, Params.EtcdTLSCACert.GetValue())
		t.Logf("tls CACert = %s", Params.EtcdTLSCACert.GetValue())

		assert.NotEmpty(t, Params.EtcdTLSCert.GetValue())
		t.Logf("tls cert = %s", Params.EtcdTLSCert.GetValue())

		assert.NotEmpty(t, Params.EtcdTLSMinVersion.GetValue())
		t.Logf("tls minVersion = %s", Params.EtcdTLSMinVersion.GetValue())

		// test UseEmbedEtcd
		t.Setenv("etcd.use.embed", "true")
		t.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.ClusterDeployMode)
		assert.Panics(t, func() {
			NewBaseTable()
		})

		t.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
		t.Setenv("etcd.use.embed", "false")
		SParams.init(bt)
	})

	t.Run("test tikvConfig", func(t *testing.T) {
		Params := &SParams.TiKVCfg

		assert.NotZero(t, len(Params.Endpoints.GetAsStrings()))
		t.Logf("tikv endpoints = %s", Params.Endpoints.GetAsStrings())

		assert.NotEqual(t, Params.MetaRootPath, "")
		t.Logf("meta root path = %s", Params.MetaRootPath.GetValue())

		assert.NotEqual(t, Params.KvRootPath, "")
		t.Logf("kv root path = %s", Params.KvRootPath.GetValue())

		t.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
		SParams.init(bt)
	})

	t.Run("test woodpeckerConfig", func(t *testing.T) {
		wpCfg := &SParams.WoodpeckerCfg
		assert.Equal(t, wpCfg.MetaType.GetValue(), "etcd")
		assert.Equal(t, wpCfg.MetaPrefix.GetValue(), "woodpecker")

		assert.Equal(t, wpCfg.AppendQueueSize.GetAsInt(), 10000)
		assert.Equal(t, wpCfg.AppendMaxRetries.GetAsInt(), 3)
		assert.Equal(t, wpCfg.SegmentRollingMaxSize.GetAsSize(), int64(256*1024*1024))
		assert.Equal(t, wpCfg.SegmentRollingMaxTime.GetAsDurationByParse().Seconds(), float64(600))
		assert.Equal(t, wpCfg.SegmentRollingMaxBlocks.GetAsInt64(), int64(1000))
		assert.Equal(t, wpCfg.AuditorMaxInterval.GetAsDurationByParse().Seconds(), float64(10))

		// Test default quorum configuration values
		// Buffer pools
		assert.Equal(t, wpCfg.QuorumBufferPoolsRegion1.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumBufferPoolsRegion2.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumBufferPoolsRegion3.GetValue(), "")

		// Selection strategy
		assert.Equal(t, wpCfg.QuorumAffinityMode.GetValue(), "soft")
		assert.Equal(t, wpCfg.QuorumReplicas.GetAsInt(), 3)
		assert.Equal(t, wpCfg.QuorumStrategy.GetValue(), "random")

		// Custom placement for all replicas (1-5)
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica1Region.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica1AZ.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica1RG.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica2Region.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica2AZ.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica2RG.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica3Region.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica3AZ.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica3RG.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica4Region.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica4AZ.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica4RG.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica5Region.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica5AZ.GetValue(), "")
		assert.Equal(t, wpCfg.QuorumCustomPlacementReplica5RG.GetValue(), "")

		assert.Equal(t, wpCfg.SyncMaxInterval.GetAsDurationByParse().Milliseconds(), int64(200))
		assert.Equal(t, wpCfg.SyncMaxIntervalForLocalStorage.GetAsDurationByParse().Milliseconds(), int64(10))
		assert.Equal(t, wpCfg.SyncMaxEntries.GetAsInt(), 10000)
		assert.Equal(t, wpCfg.SyncMaxBytes.GetAsSize(), int64(256*1024*1024))
		assert.Equal(t, wpCfg.FlushMaxRetries.GetAsInt(), 5)
		assert.Equal(t, wpCfg.FlushMaxSize.GetAsSize(), int64(2*1024*1024))
		assert.Equal(t, wpCfg.FlushMaxThreads.GetAsInt(), 32)
		assert.Equal(t, wpCfg.RetryInterval.GetAsDurationByParse().Milliseconds(), int64(1000))
		assert.Equal(t, wpCfg.CompactionSize.GetAsSize(), int64(2*1024*1024))
		assert.Equal(t, wpCfg.CompactionMaxParallelUploads.GetAsInt(), 4)
		assert.Equal(t, wpCfg.CompactionMaxParallelReads.GetAsInt(), 8)
		assert.Equal(t, wpCfg.ReaderMaxBatchSize.GetAsSize(), int64(16*1024*1024))
		assert.Equal(t, wpCfg.ReaderMaxFetchThreads.GetAsInt(), 32)

		assert.Equal(t, wpCfg.StorageType.GetValue(), "minio")
		assert.Equal(t, wpCfg.RootPath.GetValue(), "/var/lib/milvus/woodpecker")
	})

	t.Run("test woodpeckerQuorumConfig", func(t *testing.T) {
		wpCfg := &SParams.WoodpeckerCfg

		// Test setting custom quorum configuration values
		// Buffer pools for different regions
		bt.Save("woodpecker.client.quorum.quorumBufferPools.region1", "node1:8080,node2:8080")
		bt.Save("woodpecker.client.quorum.quorumBufferPools.region2", "node3:8080,node4:8080")
		bt.Save("woodpecker.client.quorum.quorumBufferPools.region3", "node5:8080,node6:8080")

		// Selection strategy
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.affinityMode", "hard")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.replicas", "5")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.strategy", "custom")

		// Custom placement for replicas
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica1.region", "region1")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica1.az", "az-1")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica1.resourceGroup", "rg-1")

		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica2.region", "region2")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica2.az", "az-2")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica2.resourceGroup", "rg-2")

		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica3.region", "region3")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica3.az", "az-3")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica3.resourceGroup", "rg-3")

		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica4.region", "region1")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica4.az", "az-4")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica4.resourceGroup", "rg-4")

		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica5.region", "region2")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica5.az", "az-5")
		bt.Save("woodpecker.client.quorum.quorumSelectStrategy.customPlacement.replica5.resourceGroup", "rg-5")

		// Reinitialize configuration to pick up the new values
		SParams.WoodpeckerCfg.QuorumBufferPoolsRegion1.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumBufferPoolsRegion2.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumBufferPoolsRegion3.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumAffinityMode.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumReplicas.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumStrategy.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica1Region.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica1AZ.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica1RG.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica2Region.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica2AZ.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica2RG.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica3Region.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica3AZ.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica3RG.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica4Region.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica4AZ.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica4RG.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica5Region.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica5AZ.Init(bt.mgr)
		SParams.WoodpeckerCfg.QuorumCustomPlacementReplica5RG.Init(bt.mgr)

		// Verify the updated configuration values
		// Buffer pools
		assert.Equal(t, "node1:8080,node2:8080", wpCfg.QuorumBufferPoolsRegion1.GetValue())
		assert.Equal(t, "node3:8080,node4:8080", wpCfg.QuorumBufferPoolsRegion2.GetValue())
		assert.Equal(t, "node5:8080,node6:8080", wpCfg.QuorumBufferPoolsRegion3.GetValue())

		// Selection strategy
		assert.Equal(t, "hard", wpCfg.QuorumAffinityMode.GetValue())
		assert.Equal(t, 5, wpCfg.QuorumReplicas.GetAsInt())
		assert.Equal(t, "custom", wpCfg.QuorumStrategy.GetValue())

		// Custom placement for replicas
		assert.Equal(t, "region1", wpCfg.QuorumCustomPlacementReplica1Region.GetValue())
		assert.Equal(t, "az-1", wpCfg.QuorumCustomPlacementReplica1AZ.GetValue())
		assert.Equal(t, "rg-1", wpCfg.QuorumCustomPlacementReplica1RG.GetValue())

		assert.Equal(t, "region2", wpCfg.QuorumCustomPlacementReplica2Region.GetValue())
		assert.Equal(t, "az-2", wpCfg.QuorumCustomPlacementReplica2AZ.GetValue())
		assert.Equal(t, "rg-2", wpCfg.QuorumCustomPlacementReplica2RG.GetValue())

		assert.Equal(t, "region3", wpCfg.QuorumCustomPlacementReplica3Region.GetValue())
		assert.Equal(t, "az-3", wpCfg.QuorumCustomPlacementReplica3AZ.GetValue())
		assert.Equal(t, "rg-3", wpCfg.QuorumCustomPlacementReplica3RG.GetValue())

		assert.Equal(t, "region1", wpCfg.QuorumCustomPlacementReplica4Region.GetValue())
		assert.Equal(t, "az-4", wpCfg.QuorumCustomPlacementReplica4AZ.GetValue())
		assert.Equal(t, "rg-4", wpCfg.QuorumCustomPlacementReplica4RG.GetValue())

		assert.Equal(t, "region2", wpCfg.QuorumCustomPlacementReplica5Region.GetValue())
		assert.Equal(t, "az-5", wpCfg.QuorumCustomPlacementReplica5AZ.GetValue())
		assert.Equal(t, "rg-5", wpCfg.QuorumCustomPlacementReplica5RG.GetValue())

		// Log the configuration values for verification
		t.Logf("Buffer pools - Region1: %s, Region2: %s, Region3: %s",
			wpCfg.QuorumBufferPoolsRegion1.GetValue(),
			wpCfg.QuorumBufferPoolsRegion2.GetValue(),
			wpCfg.QuorumBufferPoolsRegion3.GetValue())
		t.Logf("Selection strategy - Affinity: %s, Replicas: %d, Strategy: %s",
			wpCfg.QuorumAffinityMode.GetValue(),
			wpCfg.QuorumReplicas.GetAsInt(),
			wpCfg.QuorumStrategy.GetValue())
		t.Logf("Custom placement - Replica1: {region: %s, az: %s, rg: %s}",
			wpCfg.QuorumCustomPlacementReplica1Region.GetValue(),
			wpCfg.QuorumCustomPlacementReplica1AZ.GetValue(),
			wpCfg.QuorumCustomPlacementReplica1RG.GetValue())
		t.Logf("Custom placement - Replica4: {region: %s, az: %s, rg: %s}",
			wpCfg.QuorumCustomPlacementReplica4Region.GetValue(),
			wpCfg.QuorumCustomPlacementReplica4AZ.GetValue(),
			wpCfg.QuorumCustomPlacementReplica4RG.GetValue())
		t.Logf("Custom placement - Replica5: {region: %s, az: %s, rg: %s}",
			wpCfg.QuorumCustomPlacementReplica5Region.GetValue(),
			wpCfg.QuorumCustomPlacementReplica5AZ.GetValue(),
			wpCfg.QuorumCustomPlacementReplica5RG.GetValue())
	})

	t.Run("test pulsarConfig", func(t *testing.T) {
		// test default value
		{
			pc := &PulsarConfig{}
			base := &BaseTable{mgr: config.NewManager()}
			pc.Init(base)
			assert.Empty(t, pc.Address.GetValue())
		}
		{
			assert.NotEqual(t, SParams.PulsarCfg.Address.GetValue(), "")
			t.Logf("pulsar address = %s", SParams.PulsarCfg.Address.GetValue())
			assert.Equal(t, SParams.PulsarCfg.MaxMessageSize.GetAsInt(), 2097152)
		}

		address := "pulsar://localhost:6650"
		{
			bt.Save("pulsar.address", address)
			assert.Equal(t, SParams.PulsarCfg.Address.GetValue(), address)
		}

		{
			bt.Save("pulsar.address", "localhost")
			bt.Save("pulsar.port", "6650")
			assert.Equal(t, SParams.PulsarCfg.Address.GetValue(), address)
		}
	})

	t.Run("test pulsar web config", func(t *testing.T) {
		assert.NotEqual(t, SParams.PulsarCfg.Address.GetValue(), "")

		{
			assert.NotEqual(t, SParams.PulsarCfg.WebAddress.GetValue(), "")
		}

		{
			bt.Save(SParams.PulsarCfg.Address.Key, "u\\invalid")
			assert.Equal(t, SParams.PulsarCfg.WebAddress.GetValue(), "")
		}

		{
			bt.Save(SParams.PulsarCfg.Address.Key, "")
			assert.Equal(t, SParams.PulsarCfg.WebAddress.GetValue(), "")
		}
	})

	t.Run("test pulsar auth config", func(t *testing.T) {
		Params := &SParams.PulsarCfg

		assert.Equal(t, "", Params.AuthPlugin.GetValue())
		assert.Equal(t, "{}", Params.AuthParams.GetValue())
	})

	t.Run("test pulsar auth config formatter", func(t *testing.T) {
		Params := &SParams.PulsarCfg

		assert.Equal(t, "{}", Params.AuthParams.Formatter(""))
		assert.Equal(t, "{\"a\":\"b\"}", Params.AuthParams.Formatter("a:b"))
	})

	t.Run("test pulsar tenant/namespace config", func(t *testing.T) {
		Params := &SParams.PulsarCfg

		assert.Equal(t, "public", Params.Tenant.GetValue())
		assert.Equal(t, "default", Params.Namespace.GetValue())
	})

	t.Run("pulsar_operation_timeout", func(t *testing.T) {
		Params := &SParams.PulsarCfg

		assert.Equal(t, "60", Params.RequestTimeout.GetValue())
	})

	t.Run("pulsar_backlog_auto_clear_bytes", func(t *testing.T) {
		Params := &SParams.PulsarCfg
		assert.Equal(t, int64(100*1024*1024), Params.BacklogAutoClearBytes.GetAsSize())
	})

	t.Run("test rocksmqConfig", func(t *testing.T) {
		Params := &SParams.RocksmqCfg

		assert.NotEqual(t, Params.Path.GetValue(), "")
		t.Logf("rocksmq path = %s", Params.Path.GetValue())
	})

	t.Run("test kafkaConfig", func(t *testing.T) {
		// test default value
		{
			kc := &KafkaConfig{}
			base := &BaseTable{mgr: config.NewManager()}
			kc.Init(base)
			assert.Equal(t, "localhost:9092", kc.Address.GetValue())
			assert.Empty(t, kc.SaslMechanisms.GetValue())
			assert.Empty(t, kc.SecurityProtocol.GetValue())
			assert.Equal(t, kc.ReadTimeout.GetAsDuration(time.Second), 10*time.Second)
			assert.Equal(t, kc.KafkaUseSSL.GetAsBool(), false)
			assert.Empty(t, kc.KafkaTLSCACert.GetValue())
			assert.Empty(t, kc.KafkaTLSCert.GetValue())
			assert.Empty(t, kc.KafkaTLSKey.GetValue())
			assert.Empty(t, kc.KafkaTLSKeyPassword.GetValue())
		}
	})

	t.Run("test minioConfig", func(t *testing.T) {
		Params := &SParams.MinioCfg

		addr := Params.Address.GetValue()
		equal := addr == "localhost:9000" || addr == "minio:9000"
		assert.Equal(t, equal, true)
		t.Logf("minio address = %s", Params.Address.GetValue())

		assert.Equal(t, Params.AccessKeyID.GetValue(), "minioadmin")

		assert.Equal(t, Params.SecretAccessKey.GetValue(), "minioadmin")

		assert.Equal(t, Params.UseSSL.GetAsBool(), false)

		assert.NotEmpty(t, Params.SslCACert.GetValue())

		assert.Equal(t, Params.UseIAM.GetAsBool(), false)

		assert.Equal(t, Params.CloudProvider.GetValue(), "aws")

		assert.Equal(t, Params.IAMEndpoint.GetValue(), "")

		assert.Equal(t, Params.GcpCredentialJSON.GetValue(), "")

		t.Logf("Minio BucketName = %s", Params.BucketName.GetValue())

		t.Logf("Minio rootpath = %s", Params.RootPath.GetValue())
	})

	t.Run("test metastore config", func(t *testing.T) {
		Params := &SParams.MetaStoreCfg

		assert.Equal(t, util.MetaStoreTypeEtcd, Params.MetaStoreType.GetValue())
		assert.Equal(t, 86400*time.Second, Params.SnapshotTTLSeconds.GetAsDuration(time.Second))
		assert.Equal(t, 3600*time.Second, Params.SnapshotReserveTimeSeconds.GetAsDuration(time.Second))
		assert.Equal(t, 100000, Params.PaginationSize.GetAsInt())
		assert.Equal(t, 32, Params.ReadConcurrency.GetAsInt())
	})

	t.Run("test profile config", func(t *testing.T) {
		params := &SParams.ProfileCfg
		assert.Equal(t, "/var/lib/milvus/data/pprof", params.PprofPath.GetValue())
		bt.Save(params.PprofPath.Key, "/tmp/pprof")
		assert.Equal(t, "/tmp/pprof", params.PprofPath.GetValue())
	})
}

func TestRuntimConfig(t *testing.T) {
	SetRole(typeutil.StandaloneRole)
	assert.Equal(t, GetRole(), typeutil.StandaloneRole)

	SetLocalComponentEnabled(typeutil.QueryNodeRole)
	assert.True(t, IsLocalComponentEnabled(typeutil.QueryNodeRole))

	SetLocalComponentEnabled(typeutil.QueryCoordRole)
	assert.True(t, IsLocalComponentEnabled(typeutil.QueryCoordRole))
}
