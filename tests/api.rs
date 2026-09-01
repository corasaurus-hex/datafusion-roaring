use datafusion_roaring::{
    all_udafs, all_udfs, roaring_agg_udaf, roaring_cardinality_udf, roaring_contains_udf,
    roaring_difference_udf, roaring_intersection_udf, roaring_or_agg_udaf, roaring_union_udf,
    roaring_xor_udf,
};

#[test]
fn exported_function_inventory_is_complete_and_ordered() {
    let scalar_names = all_udfs()
        .into_iter()
        .map(|udf| udf.name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        scalar_names,
        [
            "roaring_cardinality",
            "roaring_contains",
            "roaring_min",
            "roaring_max",
            "roaring_rank",
            "roaring_select",
            "roaring_is_empty",
            "roaring_serialized_size",
            "roaring_contains_range",
            "roaring_range_cardinality",
            "roaring_empty",
            "roaring_from_range",
            "roaring_from_uint_list",
            "roaring_or",
            "roaring_union",
            "roaring_and",
            "roaring_intersection",
            "roaring_xor",
            "roaring_symmetric_difference",
            "roaring_andnot",
            "roaring_difference",
            "roaring_union_cardinality",
            "roaring_or_cardinality",
            "roaring_intersection_cardinality",
            "roaring_and_cardinality",
            "roaring_symmetric_difference_cardinality",
            "roaring_xor_cardinality",
            "roaring_difference_cardinality",
            "roaring_andnot_cardinality",
            "roaring_intersects",
            "roaring_is_disjoint",
            "roaring_is_subset",
            "roaring_is_superset",
            "roaring_insert",
            "roaring_remove",
            "roaring_insert_range",
            "roaring_remove_range",
            "roaring_is_valid",
            "roaring_validate",
            "roaring_to_uint_list",
        ]
    );

    let aggregate_names = all_udafs()
        .into_iter()
        .map(|udaf| udaf.name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        aggregate_names,
        ["roaring_agg", "roaring_or_agg", "roaring_union_agg"]
    );
}

#[test]
fn individual_constructors_return_the_documented_names() {
    let scalar_names = [
        roaring_cardinality_udf().name().to_owned(),
        roaring_contains_udf().name().to_owned(),
        roaring_union_udf().name().to_owned(),
        roaring_intersection_udf().name().to_owned(),
        roaring_difference_udf().name().to_owned(),
        roaring_xor_udf().name().to_owned(),
    ];
    assert_eq!(
        scalar_names,
        [
            "roaring_cardinality",
            "roaring_contains",
            "roaring_union",
            "roaring_intersection",
            "roaring_difference",
            "roaring_xor",
        ]
    );

    assert_eq!(roaring_agg_udaf().name(), "roaring_agg");
    assert_eq!(roaring_or_agg_udaf().name(), "roaring_or_agg");
}
