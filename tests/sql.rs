use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, BooleanArray, ListArray, RecordBatch, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_roaring::{all_udafs, all_udfs, decode_bitmap, encode_bitmap};
use roaring::RoaringBitmap;

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    for udf in all_udfs() {
        ctx.register_udf(udf);
    }
    for udaf in all_udafs() {
        ctx.register_udaf(udaf);
    }
    ctx
}

fn register(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("group_id", DataType::UInt32, false),
        Field::new("value", DataType::UInt32, true),
    ]));
    let first = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![0, 0, 1])),
            Arc::new(UInt32Array::from(vec![Some(1), Some(2), Some(2)])),
        ],
    )
    .unwrap();
    let second = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![0, 1])),
            Arc::new(UInt32Array::from(vec![Some(2), None])),
        ],
    )
    .unwrap();
    ctx.register_table(
        "values",
        Arc::new(MemTable::try_new(schema, vec![vec![first], vec![second]]).unwrap()),
    )
    .unwrap();
}

fn register_bitmaps(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "bitmap",
        DataType::Binary,
        true,
    )]));
    let valid = encode_bitmap(&RoaringBitmap::from_iter([1, 2])).unwrap();
    let values: Vec<Option<&[u8]>> = vec![Some(valid.as_slice()), Some(b"bad"), None];
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(BinaryArray::from(values))]).unwrap();
    ctx.register_table(
        "bitmaps",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
}

async fn query(ctx: &SessionContext, sql: &str) -> RecordBatch {
    ctx.sql(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .remove(0)
}

async fn query_error(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Ok(dataframe) => dataframe.collect().await.unwrap_err().to_string(),
        Err(error) => error.to_string(),
    }
}

#[tokio::test]
async fn aggregate_names_match_the_inventory() {
    let ctx = context();
    register(&ctx);
    let batch = query(
        &ctx,
        "WITH grouped AS (SELECT group_id, roaring_agg(value) AS bitmap FROM values GROUP BY group_id) SELECT roaring_cardinality(roaring_or_agg(bitmap)), roaring_cardinality(roaring_union_agg(bitmap)) FROM grouped",
    )
    .await;
    for column in 0..2 {
        assert_eq!(
            batch
                .column(column)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            2
        );
    }
}

#[tokio::test]
async fn aggregates_merge_partitions_and_handle_null_only_inputs() {
    let ctx = context();
    register(&ctx);
    let batch = query(
        &ctx,
        "SELECT group_id, roaring_cardinality(roaring_agg(value)) FROM values GROUP BY group_id ORDER BY group_id",
    )
    .await;
    let cardinalities = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(cardinalities.values(), &[2, 1]);

    let batch = query(
        &ctx,
        "SELECT roaring_cardinality(roaring_agg(value)) FROM values WHERE value IS NULL",
    )
    .await;
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        0
    );

    let batch = query(
        &ctx,
        "SELECT roaring_cardinality(roaring_or_agg(arrow_cast(NULL, 'Binary')))",
    )
    .await;
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        0
    );
}

#[tokio::test]
async fn ordinary_sql_integer_literals_are_coerced() {
    let ctx = context();
    register(&ctx);
    let batch = query(
        &ctx,
        "WITH b AS (SELECT roaring_agg(value) AS v FROM values) SELECT roaring_contains(v, 2), roaring_cardinality(roaring_from_range(1, 4)) FROM b",
    )
    .await;
    assert!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        3
    );
}

#[tokio::test]
async fn accessors_constructors_and_ranges_work() {
    let ctx = context();
    let batch = query(
        &ctx,
        "WITH b AS (SELECT roaring_from_range(arrow_cast(10, 'UInt32'), arrow_cast(15, 'UInt64')) AS v) SELECT roaring_cardinality(v), roaring_contains(v, arrow_cast(12, 'UInt32')), roaring_min(v), roaring_max(v), roaring_rank(v, arrow_cast(12, 'UInt32')), roaring_select(v, arrow_cast(2, 'UInt64')), roaring_is_empty(v), roaring_contains_range(v, arrow_cast(11, 'UInt32'), arrow_cast(14, 'UInt64')), roaring_range_cardinality(v, arrow_cast(12, 'UInt32'), arrow_cast(20, 'UInt64')), roaring_serialized_size(v) FROM b",
    )
    .await;
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        5
    );
    assert!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(0),
        10
    );
    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(0),
        14
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(
        batch
            .column(5)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(0),
        12
    );
    assert!(
        !batch
            .column(6)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert!(
        batch
            .column(7)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(
        batch
            .column(8)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        3
    );
    let encoded = batch
        .column(9)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert!(encoded > 6);
}

#[tokio::test]
async fn set_operations_aliases_cardinalities_and_relationships_work() {
    let ctx = context();
    let batch = query(
        &ctx,
        "WITH sets AS (SELECT roaring_from_range(arrow_cast(1, 'UInt32'), arrow_cast(4, 'UInt64')) AS a, roaring_from_range(arrow_cast(3, 'UInt32'), arrow_cast(6, 'UInt64')) AS b) SELECT roaring_union_cardinality(a,b), roaring_or_cardinality(a,b), roaring_intersection_cardinality(a,b), roaring_and_cardinality(a,b), roaring_difference_cardinality(a,b), roaring_andnot_cardinality(a,b), roaring_xor_cardinality(a,b), roaring_symmetric_difference_cardinality(a,b), roaring_intersects(a,b), roaring_is_disjoint(a,b), roaring_is_subset(roaring_and(a,b),a), roaring_is_superset(roaring_or(a,b),a) FROM sets",
    )
    .await;
    let expected = [5, 5, 1, 1, 2, 2, 4, 4];
    for (column, expected) in expected.into_iter().enumerate() {
        assert_eq!(
            batch
                .column(column)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
    assert!(
        batch
            .column(8)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert!(
        !batch
            .column(9)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert!(
        batch
            .column(10)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert!(
        batch
            .column(11)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
}

#[tokio::test]
async fn set_operation_aliases_return_the_same_bitmaps() {
    let ctx = context();
    let batch = query(
        &ctx,
        "WITH sets AS (SELECT roaring_from_range(1, 4) AS a, roaring_from_range(3, 6) AS b) SELECT roaring_or(a,b), roaring_union(a,b), roaring_and(a,b), roaring_intersection(a,b), roaring_xor(a,b), roaring_symmetric_difference(a,b), roaring_andnot(a,b), roaring_difference(a,b) FROM sets",
    )
    .await;
    let expected = [
        vec![1, 2, 3, 4, 5],
        vec![1, 2, 3, 4, 5],
        vec![3],
        vec![3],
        vec![1, 2, 4, 5],
        vec![1, 2, 4, 5],
        vec![1, 2],
        vec![1, 2],
    ];
    for (column, expected) in expected.into_iter().enumerate() {
        let bytes = batch
            .column(column)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0);
        assert_eq!(
            decode_bitmap(bytes).unwrap().iter().collect::<Vec<_>>(),
            expected
        );
    }
}

#[tokio::test]
async fn mutation_and_list_conversion_work() {
    let ctx = context();
    let batch = query(
        &ctx,
        "WITH b AS (SELECT roaring_from_uint_list(arrow_cast([1,3,3], 'List(UInt32)')) AS v), m AS (SELECT roaring_remove_range(roaring_remove(roaring_insert_range(roaring_insert(v, 2), 10, 13), 1), 11, 13) AS v FROM b) SELECT v, roaring_to_uint_list(v), roaring_is_valid(v), roaring_cardinality(roaring_validate(v)) FROM m",
    )
    .await;
    let bytes = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0);
    assert_eq!(
        decode_bitmap(bytes).unwrap().iter().collect::<Vec<_>>(),
        vec![2, 3, 10]
    );
    let lists = batch
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let list = lists.value(0);
    let values = list.as_any().downcast_ref::<UInt32Array>().unwrap();
    assert_eq!(values.values(), &[2, 3, 10]);
    assert!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        3
    );
}

#[tokio::test]
async fn empty_and_null_inputs_have_documented_results() {
    let ctx = context();
    let batch = query(
        &ctx,
        "WITH b AS (SELECT roaring_empty() AS v) SELECT roaring_cardinality(v), roaring_min(v), roaring_max(v), roaring_select(v, 0), roaring_contains_range(v, 5, 5) FROM b",
    )
    .await;
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        0
    );
    for column in 1..4 {
        assert!(batch.column(column).is_null(0));
    }
    assert!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );

    let batch = query(
        &ctx,
        "SELECT roaring_cardinality(arrow_cast(NULL, 'Binary')), roaring_contains(arrow_cast(NULL, 'Binary'), 1), roaring_from_range(arrow_cast(NULL, 'UInt32'), 2), roaring_or(arrow_cast(NULL, 'Binary'), roaring_empty()), roaring_is_valid(arrow_cast(NULL, 'Binary')), roaring_to_uint_list(arrow_cast(NULL, 'Binary'))",
    )
    .await;
    for column in 0..batch.num_columns() {
        assert!(batch.column(column).is_null(0));
    }
}

#[tokio::test]
async fn malformed_values_and_invalid_ranges_are_rejected() {
    let ctx = context();
    register_bitmaps(&ctx);
    let batch = query(&ctx, "SELECT roaring_is_valid(bitmap) FROM bitmaps").await;
    let validity = batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(validity.value(0));
    assert!(!validity.value(1));
    assert!(validity.is_null(2));

    let error = query_error(
        &ctx,
        "SELECT roaring_validate(bitmap) FROM bitmaps WHERE NOT roaring_is_valid(bitmap)",
    )
    .await;
    assert!(error.contains("could not decode roaring value"), "{error}");

    let error = query_error(&ctx, "SELECT roaring_from_range(10, 9)").await;
    assert!(error.contains("before start"), "{error}");

    let error = query_error(&ctx, "SELECT roaring_from_range(0, 4294967297)").await;
    assert!(error.contains("exceeds 2^32"), "{error}");
}

#[tokio::test]
async fn ranges_can_include_u32_max() {
    let ctx = context();
    let batch = query(
        &ctx,
        "SELECT roaring_max(roaring_from_range(arrow_cast(4294967295, 'UInt32'), arrow_cast(4294967296, 'UInt64')))",
    )
    .await;
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(0),
        u32::MAX
    );
}
