// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <chrono>
#include <cstdint>

namespace milvus::segcore {

// DeleteApplyProbe: per-thread accumulators to decompose the cost of one
// LoadDeletedRecord call (issue #49435 investigation).
//
// One LoadDeletedRecord call runs entirely on one thread:
//   LoadDeletedRecord -> LoadPush -> InternalPush -> search_pk_func_
//     (search_batch_pks: pin ts col + pin pk col + binary-search scan)
//     per-matched-row callback: read insert_ts + skiplist insert + mask set
//
// The probe is Reset() at LoadDeletedRecord entry and logged at exit, so
// accumulation from other paths (StreamPush on other threads) is isolated
// by thread_local storage.
struct DeleteApplyProbe {
    // All accumulators are in NANOSECONDS: per-row sections are often <1us,
    // so microsecond accumulation would truncate every sample to 0.
    // one-time pins in search_batch_pks
    int64_t pin_ts_ns = 0;
    int64_t pin_pk_ns = 0;
    // binary-search scan (loop wall time minus callback time)
    int64_t scan_ns = 0;
    // per-matched-row costs inside InternalPush's callback
    int64_t read_insert_ts_ns = 0;  // get_insert_timestamp_func_ / timestamps_
    int64_t skiplist_insert_ns = 0; // accessor.insert + mask set
    int64_t matched = 0;            // callback invocations

    void
    Reset() {
        *this = DeleteApplyProbe{};
    }
};

inline thread_local DeleteApplyProbe g_delete_apply_probe;

inline int64_t
ProbeNsSince(const std::chrono::steady_clock::time_point& t0) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now() - t0)
        .count();
}

}  // namespace milvus::segcore
