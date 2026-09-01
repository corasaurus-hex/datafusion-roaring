//! Roaring bitmap scalar and aggregate UDFs for Apache DataFusion.
//!
//! The SQL API uses Arrow `Binary` for 32-bit [`roaring::RoaringBitmap`] values.
//! Serialized values carry a versioned header and can be stored in Arrow IPC or Parquet.

#![deny(missing_docs)]

mod aggregate;
mod codec;
mod scalar;

use aggregate::{AggregateKind, RoaringAggregate};
use datafusion_expr::{AggregateUDF, ScalarUDF};
use scalar::{RoaringScalar, ScalarKind};

pub use codec::{decode_bitmap, encode_bitmap};

/// Builds `roaring_agg(UInt32) -> Binary`.
pub fn roaring_agg_udaf() -> AggregateUDF {
    AggregateUDF::from(RoaringAggregate::new("roaring_agg", AggregateKind::Values))
}

/// Builds `roaring_or_agg(Binary) -> Binary`.
pub fn roaring_or_agg_udaf() -> AggregateUDF {
    AggregateUDF::from(RoaringAggregate::new(
        "roaring_or_agg",
        AggregateKind::Union,
    ))
}

/// Builds every aggregate UDF exported by this crate, including documented aliases.
pub fn all_udafs() -> Vec<AggregateUDF> {
    vec![
        roaring_agg_udaf(),
        roaring_or_agg_udaf(),
        AggregateUDF::from(RoaringAggregate::new(
            "roaring_union_agg",
            AggregateKind::Union,
        )),
    ]
}

fn scalar(name: &'static str, kind: ScalarKind) -> ScalarUDF {
    ScalarUDF::from(RoaringScalar::new(name, kind))
}

/// Builds `roaring_cardinality(Binary) -> UInt64`.
pub fn roaring_cardinality_udf() -> ScalarUDF {
    scalar("roaring_cardinality", ScalarKind::Cardinality)
}

/// Builds `roaring_contains(Binary, UInt32) -> Boolean`.
pub fn roaring_contains_udf() -> ScalarUDF {
    scalar("roaring_contains", ScalarKind::Contains)
}

/// Builds `roaring_union(Binary, Binary) -> Binary`.
pub fn roaring_union_udf() -> ScalarUDF {
    scalar("roaring_union", ScalarKind::Union)
}

/// Builds `roaring_intersection(Binary, Binary) -> Binary`.
pub fn roaring_intersection_udf() -> ScalarUDF {
    scalar("roaring_intersection", ScalarKind::Intersection)
}

/// Builds `roaring_difference(Binary, Binary) -> Binary`.
pub fn roaring_difference_udf() -> ScalarUDF {
    scalar("roaring_difference", ScalarKind::Difference)
}

/// Builds `roaring_xor(Binary, Binary) -> Binary`.
pub fn roaring_xor_udf() -> ScalarUDF {
    scalar("roaring_xor", ScalarKind::Xor)
}

/// Builds every scalar UDF exported by this crate, including documented aliases.
pub fn all_udfs() -> Vec<ScalarUDF> {
    use ScalarKind::*;
    [
        ("roaring_cardinality", Cardinality),
        ("roaring_contains", Contains),
        ("roaring_min", Min),
        ("roaring_max", Max),
        ("roaring_rank", Rank),
        ("roaring_select", Select),
        ("roaring_is_empty", IsEmpty),
        ("roaring_serialized_size", SerializedSize),
        ("roaring_contains_range", ContainsRange),
        ("roaring_range_cardinality", RangeCardinality),
        ("roaring_empty", Empty),
        ("roaring_from_range", FromRange),
        ("roaring_from_uint_list", FromList),
        ("roaring_or", Union),
        ("roaring_union", Union),
        ("roaring_and", Intersection),
        ("roaring_intersection", Intersection),
        ("roaring_xor", Xor),
        ("roaring_symmetric_difference", Xor),
        ("roaring_andnot", Difference),
        ("roaring_difference", Difference),
        ("roaring_union_cardinality", UnionCardinality),
        ("roaring_or_cardinality", UnionCardinality),
        ("roaring_intersection_cardinality", IntersectionCardinality),
        ("roaring_and_cardinality", IntersectionCardinality),
        ("roaring_symmetric_difference_cardinality", XorCardinality),
        ("roaring_xor_cardinality", XorCardinality),
        ("roaring_difference_cardinality", DifferenceCardinality),
        ("roaring_andnot_cardinality", DifferenceCardinality),
        ("roaring_intersects", Intersects),
        ("roaring_is_disjoint", IsDisjoint),
        ("roaring_is_subset", IsSubset),
        ("roaring_is_superset", IsSuperset),
        ("roaring_insert", Insert),
        ("roaring_remove", Remove),
        ("roaring_insert_range", InsertRange),
        ("roaring_remove_range", RemoveRange),
        ("roaring_is_valid", IsValid),
        ("roaring_validate", Validate),
        ("roaring_to_uint_list", ToList),
    ]
    .into_iter()
    .map(|(name, kind)| scalar(name, kind))
    .collect()
}
