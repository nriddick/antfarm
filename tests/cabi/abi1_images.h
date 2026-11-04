/* ABI 1 images of the option structures that grew in ABI 2, byte for byte as
   cabi/abi1.lock recorded them. The library accepts them by their own size
   and version, which is what keeps ABI 1 programs working; the lock no longer
   pins these two layouts, this header and the tests that fill it do. */
#ifndef AF_TEST_ABI1_IMAGES_H
#define AF_TEST_ABI1_IMAGES_H
#include "antfarm_groups.h"
#include <stddef.h>
#include <string.h>
#ifdef __cplusplus
#define AF1_ASSERT(c, m) static_assert(c, m)
#else
#define AF1_ASSERT(c, m) _Static_assert(c, m)
#endif
typedef struct af1_host_options {
    uint32_t struct_size, abi_version;
    uint32_t workers, caller_consumes, caller_placement, idle;
    uint64_t idle_timeout_ns;
    uint32_t ring_mib, segments, extra_small_producers, bulk_producers;
    uint64_t bulk_quota_words, small_quota_words;
    uint32_t extra_consumers, small_table_threshold, huge_pages, skip_smt_siblings;
    uint64_t reserved;
} af1_host_options;
typedef struct af1_group_options {
    uint32_t struct_size, abi_version;
    af1_host_options host;
    uint32_t group_mode, group_index, farm_per_group, reserved;
} af1_group_options;
AF1_ASSERT(sizeof(af1_host_options) == 88 && offsetof(af1_host_options, idle_timeout_ns) == 24 &&
           offsetof(af1_host_options, reserved) == 80, "ABI 1 host options image");
AF1_ASSERT(sizeof(af1_group_options) == 112 && offsetof(af1_group_options, group_mode) == 96,
           "ABI 1 group options image");
/* The ABI 1 image of the current defaults. ABI 2 appended its fields after
   reserved, so the prefix of the ABI 2 default is the image. */
static inline void af1_host_options_default(af1_host_options *out) {
    af_host_options current; af_host_options_default(&current);
    memcpy(out, &current, sizeof *out);
    out->struct_size = (uint32_t) sizeof *out; out->abi_version = 1;
}
static inline void af1_group_options_default(af1_group_options *out) {
    af_group_options current; af_group_options_default(&current);
    out->struct_size = (uint32_t) sizeof *out; out->abi_version = 1;
    memcpy(&out->host, &current.host, sizeof out->host);
    out->host.struct_size = (uint32_t) sizeof out->host; out->host.abi_version = 1;
    out->group_mode = current.group_mode; out->group_index = current.group_index;
    out->farm_per_group = current.farm_per_group; out->reserved = current.reserved;
}
#endif
