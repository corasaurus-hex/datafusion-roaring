# datafusion-roaring

Roaring bitmap UDFs for Apache DataFusion, with SQL aliases for common bitmap operation names.

The crate exports constructors and leaves registration to the caller. It does not create or mutate a `SessionContext`.

[API documentation](https://docs.rs/datafusion-roaring) is built by docs.rs for every published release.

## Installation

```toml
[dependencies]
datafusion = "55.0.0"
datafusion-roaring = "0.1.0"
```

`datafusion-roaring` 0.1.x uses DataFusion 55, Arrow 59, and Rust 1.94 or newer.

```rust
use datafusion::prelude::SessionContext;

let ctx = SessionContext::new();
for udf in datafusion_roaring::all_udfs() {
    ctx.register_udf(udf);
}
for udaf in datafusion_roaring::all_udafs() {
    ctx.register_udaf(udaf);
}
```

## SQL functions

The 0.1 API is a portable 32-bit surface. `Binary` is DataFusion and Arrow's byte-string type. Bitmap members are `UInt32`. Counts and exclusive range ends are `UInt64`.

| Category          | Functions                                                                                                                                                                                                       |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Aggregates        | `roaring_agg`, `roaring_or_agg`, `roaring_union_agg`                                                                                                                                                            |
| Accessors         | `roaring_cardinality`, `roaring_contains`, `roaring_min`, `roaring_max`, `roaring_rank`, `roaring_select`, `roaring_is_empty`, `roaring_serialized_size`, `roaring_contains_range`, `roaring_range_cardinality` |
| Constructors      | `roaring_empty`, `roaring_from_range`, `roaring_from_uint_list`                                                                                                                                                 |
| Set operations    | `roaring_or`/`roaring_union`, `roaring_and`/`roaring_intersection`, `roaring_xor`/`roaring_symmetric_difference`, `roaring_andnot`/`roaring_difference`                                                         |
| Set cardinalities | The eight corresponding `*_cardinality` aliases                                                                                                                                                                 |
| Relationships     | `roaring_intersects`, `roaring_is_disjoint`, `roaring_is_subset`, `roaring_is_superset`                                                                                                                         |
| Mutation          | `roaring_insert`, `roaring_remove`, `roaring_insert_range`, `roaring_remove_range`                                                                                                                              |
| Validation        | `roaring_is_valid`, `roaring_validate`                                                                                                                                                                          |
| Conversion        | `roaring_to_uint_list`                                                                                                                                                                                          |

Ranges are half-open: `start` is included and `end` is excluded. An end of `4294967296` is accepted so a range can include `UInt32::MAX`.

Aggregates ignore null input values and collapse duplicates. Scalar functions return null when any required argument is null, except `roaring_is_valid`, which returns `false` for malformed non-null input. The serialized representation has a versioned header followed by the portable format emitted by the `roaring` crate.

```sql
WITH sets AS (
  SELECT group_id, roaring_agg(user_id) AS users
  FROM events
  GROUP BY group_id
)
SELECT group_id, roaring_cardinality(users), roaring_contains(users, 42)
FROM sets;
```

The 0.1 API intentionally omits unchecked decoding, full-universe construction, byte/row/file conversion, and compression controls. It also omits limited/ranged list conversion, statistics, and intersection/XOR aggregates.

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
