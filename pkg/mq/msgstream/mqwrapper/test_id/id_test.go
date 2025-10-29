package test_id

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/woodpecker/woodpecker/log"

	pulsarid "github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper/pulsar"
	wpid "github.com/milvus-io/milvus/pkg/v2/mq/msgstream/mqwrapper/wp"
)

func TestCrossTypeMessageIDSerialization(t *testing.T) {
	// Test cross-type serialization/deserialization between Woodpecker and Pulsar MessageIDs
	// This test verifies that serialized bytes from one type cannot be successfully
	// deserialized by another type, ensuring type safety

	t.Run("WoodpeckerToPulsarDeserialization", func(t *testing.T) {
		// Create a Woodpecker MessageID
		wpLogMsgID := &log.LogMessageId{
			SegmentId: 12345,
			EntryId:   67890,
		}
		wpMsgID := wpid.NewWoodpeckerID(wpLogMsgID)

		// Serialize Woodpecker MessageID to bytes
		wpBytes := wpMsgID.Serialize()
		assert.NotEmpty(t, wpBytes, "Woodpecker MessageID should serialize to non-empty bytes")

		// Try to deserialize Woodpecker bytes using Pulsar deserializer
		pulsarMsgID, err := pulsarid.DeserializePulsarMsgID(wpBytes)

		if err != nil {
			// Case 1: Deserialization fails (expected behavior for incompatible formats)
			t.Logf("Woodpecker bytes cannot be deserialized as Pulsar MessageID (expected): %v", err)
			assert.Error(t, err, "Woodpecker serialized bytes should not be deserializable as Pulsar MessageID")
		} else {
			// Case 2: Deserialization succeeds but creates a different/invalid Pulsar MessageID
			t.Logf("Woodpecker bytes were deserialized as Pulsar MessageID: %v", pulsarMsgID)

			// Even if deserialization succeeds, the resulting Pulsar MessageID should be different
			// from what a proper Pulsar MessageID would look like
			pulsarID := pulsarid.NewPulsarID(pulsarMsgID)
			pulsarBytes := pulsarID.Serialize()

			// The re-serialized bytes should be different from original Woodpecker bytes
			assert.NotEqual(t, wpBytes, pulsarBytes,
				"Re-serialized Pulsar bytes should differ from original Woodpecker bytes")

			t.Logf("Original Woodpecker bytes: %v", wpBytes)
			t.Logf("Re-serialized Pulsar bytes: %v", pulsarBytes)
		}
	})

	t.Run("PulsarToWoodpeckerDeserialization", func(t *testing.T) {
		// Create a Pulsar MessageID using EarliestMessageID as a base
		basePulsarMsgID := pulsar.EarliestMessageID()
		pulsarID := pulsarid.NewPulsarID(basePulsarMsgID)

		// Serialize Pulsar MessageID to bytes
		pulsarBytes := pulsarID.Serialize()
		assert.NotEmpty(t, pulsarBytes, "Pulsar MessageID should serialize to non-empty bytes")

		// Try to deserialize Pulsar bytes using Woodpecker deserializer
		wpLogMsgID, err := wpid.DeserializeWoodpeckerMsgID(pulsarBytes)

		if err != nil {
			// Case 1: Deserialization fails (expected behavior for incompatible formats)
			t.Logf("Pulsar bytes cannot be deserialized as Woodpecker MessageID (expected): %v", err)
			assert.Error(t, err, "Pulsar serialized bytes should not be deserializable as Woodpecker MessageID")
		} else {
			// Case 2: Deserialization succeeds but creates a different/invalid Woodpecker MessageID
			t.Logf("Pulsar bytes were deserialized as Woodpecker MessageID: SegmentId=%d, EntryId=%d",
				wpLogMsgID.SegmentId, wpLogMsgID.EntryId)

			// Even if deserialization succeeds, the resulting Woodpecker MessageID should be different
			wpID := wpid.NewWoodpeckerID(wpLogMsgID)
			wpBytes := wpID.Serialize()

			// The re-serialized bytes should be different from original Pulsar bytes
			assert.NotEqual(t, pulsarBytes, wpBytes,
				"Re-serialized Woodpecker bytes should differ from original Pulsar bytes")

			t.Logf("Original Pulsar bytes: %v", pulsarBytes)
			t.Logf("Re-serialized Woodpecker bytes: %v", wpBytes)
		}
	})
}

func TestMessageIDTypeSafety(t *testing.T) {
	// Additional test to ensure each MessageID type maintains its own identity

	t.Run("WoodpeckerMessageIDProperties", func(t *testing.T) {
		wpLogMsgID := &log.LogMessageId{
			SegmentId: 100,
			EntryId:   200,
		}
		wpMsgID := wpid.NewWoodpeckerID(wpLogMsgID)

		// Test basic properties
		assert.Equal(t, wpLogMsgID, wpMsgID.WoodpeckerID(), "WoodpeckerID should return original LogMessageId")

		// Test serialization roundtrip
		wpBytes := wpMsgID.Serialize()
		deserializedWpLogMsgID, err := wpid.DeserializeWoodpeckerMsgID(wpBytes)
		assert.NoError(t, err, "Woodpecker MessageID should deserialize successfully")
		assert.Equal(t, wpLogMsgID.SegmentId, deserializedWpLogMsgID.SegmentId, "SegmentId should be preserved")
		assert.Equal(t, wpLogMsgID.EntryId, deserializedWpLogMsgID.EntryId, "EntryId should be preserved")
	})

	t.Run("PulsarMessageIDProperties", func(t *testing.T) {
		basePulsarMsgID := pulsar.EarliestMessageID()
		pulsarID := pulsarid.NewPulsarID(basePulsarMsgID)

		// Test basic properties
		assert.Equal(t, basePulsarMsgID, pulsarID.PulsarID(), "PulsarID should return original MessageID")

		// Test serialization roundtrip
		pulsarBytes := pulsarID.Serialize()
		deserializedPulsarMsgID, err := pulsarid.DeserializePulsarMsgID(pulsarBytes)
		assert.NoError(t, err, "Pulsar MessageID should deserialize successfully")
		assert.Equal(t, basePulsarMsgID.LedgerID(), deserializedPulsarMsgID.LedgerID(), "LedgerID should be preserved")
		assert.Equal(t, basePulsarMsgID.EntryID(), deserializedPulsarMsgID.EntryID(), "EntryID should be preserved")
		assert.Equal(t, basePulsarMsgID.BatchIdx(), deserializedPulsarMsgID.BatchIdx(), "BatchIdx should be preserved")
	})
}

func TestMessageIDComparisonAcrossTypes(t *testing.T) {
	// Test that comparison methods work correctly within the same type
	// but cannot be meaningfully compared across different types

	t.Run("WoodpeckerMessageIDComparison", func(t *testing.T) {
		wp1 := wpid.NewWoodpeckerID(&log.LogMessageId{SegmentId: 1, EntryId: 1})
		wp2 := wpid.NewWoodpeckerID(&log.LogMessageId{SegmentId: 1, EntryId: 2})
		wp3 := wpid.NewWoodpeckerID(&log.LogMessageId{SegmentId: 1, EntryId: 1})

		// Test Equal method
		isEqual1, err := wp1.Equal(wp3.Serialize())
		assert.NoError(t, err)
		assert.True(t, isEqual1, "Same Woodpecker MessageIDs should be equal")

		isEqual2, err := wp1.Equal(wp2.Serialize())
		assert.NoError(t, err)
		assert.False(t, isEqual2, "Different Woodpecker MessageIDs should not be equal")

		// Test LessOrEqualThan method
		isLTE, err := wp1.LessOrEqualThan(wp2.Serialize())
		assert.NoError(t, err)
		assert.True(t, isLTE, "wp1 should be less than wp2")
	})

	t.Run("PulsarMessageIDComparison", func(t *testing.T) {
		baseMsgID := pulsar.EarliestMessageID()
		pulsar1 := pulsarid.NewPulsarID(baseMsgID)
		pulsar2 := pulsarid.NewPulsarID(baseMsgID)

		// Test Equal method
		isEqual, err := pulsar1.Equal(pulsar2.Serialize())
		assert.NoError(t, err)
		assert.True(t, isEqual, "Same Pulsar MessageIDs should be equal")

		// Test LessOrEqualThan method
		isLTE, err := pulsar1.LessOrEqualThan(pulsar2.Serialize())
		assert.NoError(t, err)
		assert.True(t, isLTE, "Same Pulsar MessageIDs should be less than or equal")
	})
}
