package segments

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestRepro_LoadDeletedRecord26s sweeps segment row count and times LoadDeltaData
// (= the CGO LoadDeletedRecord apply, issue #49435). Goal: find which characteristic
// makes the per-row apply blow up to ~110us/row (26s for 237K).
//
//	REPRO_MMAP=1 forces mmap'd columns (closer to a freshly-loaded prod segment).
func TestRepro_LoadDeletedRecord26s(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mmap := os.Getenv("REPRO_MMAP") == "1"
	if mmap {
		paramtable.Get().Save("queryNode.mmap.mmapEnabled", "true")
		defer paramtable.Get().Reset("queryNode.mmap.mmapEnabled")
	}

	rootPath := t.Name()
	cmf := storage.NewTestChunkManagerFactory(paramtable.Get(), rootPath)
	cm, _ := cmf.NewPersistentStorageChunkManager(ctx)
	initcore.InitRemoteChunkManager(paramtable.Get())
	initcore.InitLocalChunkManager(filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole))
	initcore.InitMmapManager(paramtable.Get(), 1)
	initcore.InitTieredStorage(paramtable.Get())

	const collID, partID = int64(100), int64(10)
	mgr := NewManager()
	schema := mock_segcore.GenTestCollectionSchema("repro", schemapb.DataType_Int64, true)
	indexMeta := mock_segcore.GenTestIndexMeta(collID, schema)
	mgr.Collection.PutOrRef(collID, schema, indexMeta, &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection, CollectionID: collID, PartitionIDs: []int64{partID},
	})
	coll := mgr.Collection.Get(collID)

	t.Logf("=== mmap=%v ===", mmap)
	for _, N := range []int{50000, 100000, 200000} {
		segID := int64(1000 + N)
		sealed, err := NewSegment(ctx, coll, mgr.Segment, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
			CollectionID:  collID,
			SegmentID:     segID,
			PartitionID:   partID,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collID),
			Level:         datapb.SegmentLevel_Legacy,
			IsSorted:      true,
			NumOfRows:     int64(N),
		})
		require.NoError(t, err)

		binlogs, _, err := mock_segcore.SaveBinLog(ctx, collID, partID, segID, N, schema, cm)
		require.NoError(t, err)
		g, err := sealed.(*LocalSegment).StartLoadData()
		require.NoError(t, err)
		for _, bl := range binlogs {
			require.NoError(t, sealed.(*LocalSegment).LoadFieldData(ctx, bl.FieldID, int64(N), bl))
		}
		g.Done(nil)

		// delete every row: PK 0..N-1 (sequential, matches GenInsertData), ts well above insert ts
		dd := storage.NewDeltaData(int64(N))
		for i := 0; i < N; i++ {
			require.NoError(t, dd.Append(storage.NewInt64PrimaryKey(int64(i)), uint64(2_000_000)+uint64(i)))
		}

		t0 := time.Now()
		require.NoError(t, sealed.(*LocalSegment).LoadDeltaData(ctx, dd))
		el := time.Since(t0)
		t.Logf("N=%-7d deletes=%-7d  LoadDeltaData=%-13v  %.2f us/row", N, N, el, float64(el.Microseconds())/float64(N))
		sealed.Release(ctx)
	}
}

// TestRepro_LoadDeletedRecord_V2 writes REAL StorageV2 packed data to storage and loads
// the segment through the V2 path, then times LoadDeltaData — to see if V2 (packed) makes
// the same LoadDeletedRecord slow, vs the fast V1 in-memory case above.
func TestRepro_LoadDeletedRecord_V2(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	// REPRO_COLD=1 disables scalar-field cache warmup, so the PK/TS column cells are
	// NOT materialized at load time — the delete-apply's binary search is their first
	// access and triggers on-demand cachinglayer reads from MinIO (prod CacheSlot.h:558).
	cold := os.Getenv("REPRO_COLD") == "1"
	if cold {
		paramtable.Get().Save("queryNode.segcore.tieredStorage.warmup.scalarField", "disable")
		defer paramtable.Get().Reset("queryNode.segcore.tieredStorage.warmup.scalarField")
	}
	t.Logf("=== cold(no scalar warmup)=%v ===", cold)

	rootPath := t.Name()
	cmf := storage.NewTestChunkManagerFactory(paramtable.Get(), rootPath)
	cm, err := cmf.NewPersistentStorageChunkManager(ctx)
	require.NoError(t, err)
	require.NoError(t, initcore.InitRemoteChunkManager(paramtable.Get()))
	require.NoError(t, initcore.InitStorageV2FileSystem(paramtable.Get()))
	initcore.InitLocalChunkManager(filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole))
	initcore.InitMmapManager(paramtable.Get(), 1)
	initcore.InitTieredStorage(paramtable.Get())

	const collID, partID = int64(100), int64(10)
	mgr := NewManager()
	schema := mock_segcore.GenTestCollectionSchema("reproV2", schemapb.DataType_Int64, true)
	indexMeta := mock_segcore.GenTestIndexMeta(collID, schema)
	mgr.Collection.PutOrRef(collID, schema, indexMeta, &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection, CollectionID: collID, PartitionIDs: []int64{partID},
	})
	coll := mgr.Collection.Get(collID)

	var pkFieldID int64
	for _, f := range schema.Fields {
		if f.IsPrimaryKey {
			pkFieldID = f.FieldID
		}
	}

	for _, N := range []int{50000, 200000} {
		segID := int64(2000 + N)

		// build N rows with PK sequential 0..N-1
		insertData, err := mock_segcore.GenInsertData(N, schema)
		require.NoError(t, err)
		values := make([]*storage.Value, N)
		for i := 0; i < N; i++ {
			m := make(map[int64]any, len(insertData.Data))
			for fid, fd := range insertData.Data {
				m[fid] = fd.GetRow(i)
			}
			m[pkFieldID] = int64(i)
			values[i] = &storage.Value{ID: int64(i), PK: storage.NewInt64PrimaryKey(int64(i)), Timestamp: int64(i), Value: m}
		}
		rec, err := storage.ValueSerializer(values, schema)
		require.NoError(t, err)

		// write REAL StorageV2 packed data
		logIDAlloc := allocator.NewLocalAllocator(int64(N)*1000+1, math.MaxInt64)
		w, err := storage.NewBinlogRecordWriter(ctx, collID, partID, segID, schema, logIDAlloc, uint64(64*1024*1024), int64(N),
			storage.WithVersion(storage.StorageV2),
			storage.WithStorageConfig(packed.CreateStorageConfig()),
			storage.WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
				for k, v := range kvs {
					if err := cm.Write(ctx, k, v); err != nil {
						return err
					}
				}
				return nil
			}),
		)
		require.NoError(t, err)
		require.NoError(t, w.Write(rec))
		require.NoError(t, w.Close())
		fieldBinlogs, statsLog, _, _, _ := w.GetLogs()

		binlogPaths := make([]*datapb.FieldBinlog, 0, len(fieldBinlogs))
		for _, fb := range fieldBinlogs {
			binlogPaths = append(binlogPaths, fb)
		}
		loadInfo := &querypb.SegmentLoadInfo{
			CollectionID:   collID,
			SegmentID:      segID,
			PartitionID:    partID,
			InsertChannel:  fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collID),
			Level:          datapb.SegmentLevel_Legacy,
			IsSorted:       true,
			NumOfRows:      int64(N),
			StorageVersion: storage.StorageV2,
			BinlogPaths:    binlogPaths,
			Statslogs:      []*datapb.FieldBinlog{statsLog},
		}

		sealed, err := NewSegment(ctx, coll, mgr.Segment, SegmentTypeSealed, 0, loadInfo)
		require.NoError(t, err)
		g, err := sealed.(*LocalSegment).StartLoadData()
		require.NoError(t, err)
		for _, fb := range binlogPaths {
			require.NoError(t, sealed.(*LocalSegment).LoadFieldData(ctx, fb.FieldID, int64(N), fb))
		}
		g.Done(nil)

		// REPRO_NOMATCH=1: delete PKs N..2N-1 (outside the segment's 0..N-1 range)
		// so the binary-search scan runs fully but NO callback/skiplist-insert fires.
		//   warm+nomatch = SCAN only ; warm+match = SCAN+SKIPLIST ; cold+* adds PIN.
		base := int64(0)
		nomatch := os.Getenv("REPRO_NOMATCH") == "1"
		if nomatch {
			base = int64(N)
		}
		dd := storage.NewDeltaData(int64(N))
		for i := 0; i < N; i++ {
			require.NoError(t, dd.Append(storage.NewInt64PrimaryKey(base+int64(i)), uint64(2_000_000)+uint64(i)))
		}
		t0 := time.Now()
		require.NoError(t, sealed.(*LocalSegment).LoadDeltaData(ctx, dd))
		el := time.Since(t0)
		t.Logf("[V2] N=%-7d cold=%-5v nomatch=%-5v LoadDeltaData=%-13v  %.2f us/row",
			N, cold, nomatch, el, float64(el.Microseconds())/float64(N))
		sealed.Release(ctx)
	}
}

// TestRepro_LoadDeletedRecord_Contention: can local contention stretch the
// per-row apply cost (prod: ~110us/row -> 237K rows = 22-26s)?
//
// REPRO_STAGE selects contention level:
//
//	0 = baseline: 1 apply, quiet process
//	1 = 8 concurrent applies (mirrors prod DynamicPool 8/8)
//	2 = stage1 + cachinglayer churn (4 goroutines looping segment load+release)
//	3 = stage2 + CPU oversubscription (2x NumCPU busy-loop goroutines)
//
// Per-step decomposition comes from the C++ [DeleteApplyProbe] log lines
// (pin_ts / pin_pk / scan / read_insert_ts / skiplist_insert, all in us).
func TestRepro_LoadDeletedRecord_Contention(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	stage := 3
	if s := os.Getenv("REPRO_STAGE"); s != "" {
		stage, _ = strconv.Atoi(s)
	}

	rootPath := "TestRepro_LoadDeletedRecord_V2" // reuse the V2 harness root
	cmf := storage.NewTestChunkManagerFactory(paramtable.Get(), rootPath)
	cm, err := cmf.NewPersistentStorageChunkManager(ctx)
	require.NoError(t, err)
	require.NoError(t, initcore.InitRemoteChunkManager(paramtable.Get()))
	require.NoError(t, initcore.InitStorageV2FileSystem(paramtable.Get()))
	initcore.InitLocalChunkManager(filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole))
	initcore.InitMmapManager(paramtable.Get(), 1)
	initcore.InitTieredStorage(paramtable.Get())

	const collID, partID = int64(100), int64(10)
	mgr := NewManager()
	schema := mock_segcore.GenTestCollectionSchema("reproV2", schemapb.DataType_Int64, true)
	indexMeta := mock_segcore.GenTestIndexMeta(collID, schema)
	mgr.Collection.PutOrRef(collID, schema, indexMeta, &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection, CollectionID: collID, PartitionIDs: []int64{partID},
	})
	coll := mgr.Collection.Get(collID)

	var pkFieldID int64
	for _, f := range schema.Fields {
		if f.IsPrimaryKey {
			pkFieldID = f.FieldID
		}
	}

	const N = 200000
	const applyK = 8

	// ---- write ONE StorageV2 dataset (N rows, PK 0..N-1); all segments share it ----
	insertData, err := mock_segcore.GenInsertData(N, schema)
	require.NoError(t, err)
	values := make([]*storage.Value, N)
	for i := 0; i < N; i++ {
		m := make(map[int64]any, len(insertData.Data))
		for fid, fd := range insertData.Data {
			m[fid] = fd.GetRow(i)
		}
		m[pkFieldID] = int64(i)
		values[i] = &storage.Value{ID: int64(i), PK: storage.NewInt64PrimaryKey(int64(i)), Timestamp: int64(i), Value: m}
	}
	rec, err := storage.ValueSerializer(values, schema)
	require.NoError(t, err)
	logIDAlloc := allocator.NewLocalAllocator(int64(N)*1000+1, math.MaxInt64)
	w, err := storage.NewBinlogRecordWriter(ctx, collID, partID, 900000, schema, logIDAlloc, uint64(64*1024*1024), int64(N),
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(packed.CreateStorageConfig()),
		storage.WithUploader(func(ctx context.Context, kvs map[string][]byte) error {
			for k, v := range kvs {
				if err := cm.Write(ctx, k, v); err != nil {
					return err
				}
			}
			return nil
		}),
	)
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())
	fieldBinlogs, statsLog, _, _, _ := w.GetLogs()
	binlogPaths := make([]*datapb.FieldBinlog, 0, len(fieldBinlogs))
	for _, fb := range fieldBinlogs {
		binlogPaths = append(binlogPaths, fb)
	}

	mkLoadInfo := func(segID int64) *querypb.SegmentLoadInfo {
		return &querypb.SegmentLoadInfo{
			CollectionID:   collID,
			SegmentID:      segID,
			PartitionID:    partID,
			InsertChannel:  fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collID),
			Level:          datapb.SegmentLevel_Legacy,
			IsSorted:       true,
			NumOfRows:      int64(N),
			StorageVersion: storage.StorageV2,
			BinlogPaths:    binlogPaths,
			Statslogs:      []*datapb.FieldBinlog{statsLog},
		}
	}
	loadSealed := func(segID int64) Segment {
		sealed, err := NewSegment(ctx, coll, mgr.Segment, SegmentTypeSealed, 0, mkLoadInfo(segID))
		if err != nil {
			t.Error(err)
			return nil
		}
		g, err := sealed.(*LocalSegment).StartLoadData()
		if err != nil {
			t.Error(err)
			return nil
		}
		for _, fb := range binlogPaths {
			if err := sealed.(*LocalSegment).LoadFieldData(ctx, fb.FieldID, int64(N), fb); err != nil {
				t.Error(err)
				g.Done(err)
				return nil
			}
		}
		g.Done(nil)
		return sealed
	}

	// ---- load apply targets ----
	conc := 1
	if stage >= 1 {
		conc = applyK
	}
	applySegs := make([]Segment, conc)
	for i := 0; i < conc; i++ {
		applySegs[i] = loadSealed(int64(910000 + i))
		require.NotNil(t, applySegs[i])
	}

	// ---- background contention ----
	stop := make(chan struct{})
	var wgBg sync.WaitGroup
	if stage >= 2 {
		for c := 0; c < 4; c++ {
			wgBg.Add(1)
			go func(ci int) {
				defer wgBg.Done()
				it := 0
				for {
					select {
					case <-stop:
						return
					default:
					}
					seg := loadSealed(int64(920000 + ci*1000 + it%7))
					if seg != nil {
						seg.Release(ctx)
					}
					it++
				}
			}(c)
		}
	}
	if stage >= 3 {
		for c := 0; c < 2*runtime.NumCPU(); c++ {
			wgBg.Add(1)
			go func() {
				defer wgBg.Done()
				x := 1.0
				for {
					select {
					case <-stop:
						return
					default:
					}
					for i := 0; i < 1<<18; i++ {
						x = x*1.000000001 + 1e-9
					}
				}
			}()
		}
	}
	// let background traffic ramp up
	if stage >= 2 {
		time.Sleep(3 * time.Second)
	}

	// ---- concurrent applies ----
	var wg sync.WaitGroup
	durs := make([]time.Duration, conc)
	for a := 0; a < conc; a++ {
		wg.Add(1)
		go func(ai int) {
			defer wg.Done()
			dd := storage.NewDeltaData(int64(N))
			for i := 0; i < N; i++ {
				if err := dd.Append(storage.NewInt64PrimaryKey(int64(i)), uint64(2_000_000)+uint64(i)); err != nil {
					t.Error(err)
					return
				}
			}
			t0 := time.Now()
			if err := applySegs[ai].(*LocalSegment).LoadDeltaData(ctx, dd); err != nil {
				t.Error(err)
				return
			}
			durs[ai] = time.Since(t0)
		}(a)
	}
	wg.Wait()
	close(stop)
	wgBg.Wait()

	var maxD, sumD time.Duration
	for i, d := range durs {
		t.Logf("[CONTENTION stage=%d] apply#%d N=%d LoadDeltaData=%-13v %.2f us/row",
			stage, i, N, d, float64(d.Microseconds())/float64(N))
		sumD += d
		if d > maxD {
			maxD = d
		}
	}
	t.Logf("[CONTENTION stage=%d] conc=%d avg=%v max=%v", stage, conc, sumD/time.Duration(conc), maxD)
	for i := 0; i < conc; i++ {
		if applySegs[i] != nil {
			applySegs[i].Release(ctx)
		}
	}
}
