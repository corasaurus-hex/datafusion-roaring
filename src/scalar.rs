use std::ops::Bound;
use std::sync::{Arc, LazyLock};

use arrow_array::builder::{BinaryBuilder, ListBuilder, UInt32Builder};
use arrow_array::{Array, BinaryArray, BooleanArray, ListArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use roaring::RoaringBitmap;

use crate::codec::{decode_bitmap, encode_bitmap};

static SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::user_defined(Volatility::Immutable));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ScalarKind {
    Cardinality,
    Contains,
    Min,
    Max,
    Rank,
    Select,
    IsEmpty,
    SerializedSize,
    ContainsRange,
    RangeCardinality,
    Empty,
    FromRange,
    FromList,
    Union,
    Intersection,
    Difference,
    Xor,
    UnionCardinality,
    IntersectionCardinality,
    DifferenceCardinality,
    XorCardinality,
    Intersects,
    IsDisjoint,
    IsSubset,
    IsSuperset,
    Insert,
    Remove,
    InsertRange,
    RemoveRange,
    ToList,
    IsValid,
    Validate,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RoaringScalar {
    name: &'static str,
    kind: ScalarKind,
}

impl RoaringScalar {
    pub(crate) const fn new(name: &'static str, kind: ScalarKind) -> Self {
        Self { name, kind }
    }

    fn output_type(&self) -> DataType {
        match self.kind {
            ScalarKind::Cardinality
            | ScalarKind::Rank
            | ScalarKind::SerializedSize
            | ScalarKind::RangeCardinality
            | ScalarKind::UnionCardinality
            | ScalarKind::IntersectionCardinality
            | ScalarKind::DifferenceCardinality
            | ScalarKind::XorCardinality => DataType::UInt64,
            ScalarKind::Contains
            | ScalarKind::ContainsRange
            | ScalarKind::IsEmpty
            | ScalarKind::Intersects
            | ScalarKind::IsDisjoint
            | ScalarKind::IsSubset
            | ScalarKind::IsSuperset
            | ScalarKind::IsValid => DataType::Boolean,
            ScalarKind::Min | ScalarKind::Max | ScalarKind::Select => DataType::UInt32,
            ScalarKind::ToList => {
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, false)))
            }
            _ => DataType::Binary,
        }
    }
}

fn expect_arity(name: &str, actual: &[DataType], arity: usize) -> Result<()> {
    if actual.len() != arity {
        return exec_err!("{name} expects {arity} argument(s), got {}", actual.len());
    }
    Ok(())
}

impl ScalarUDFImpl for RoaringScalar {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.output_type())
    }

    fn return_field_from_args(&self, _: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), self.output_type(), true)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        use ScalarKind::*;
        let types = match self.kind {
            Empty => {
                expect_arity(self.name(), arg_types, 0)?;
                vec![]
            }
            Cardinality | Min | Max | IsEmpty | SerializedSize | ToList | IsValid | Validate => {
                expect_arity(self.name(), arg_types, 1)?;
                vec![DataType::Binary]
            }
            Contains | Rank | Insert | Remove => {
                expect_arity(self.name(), arg_types, 2)?;
                vec![DataType::Binary, DataType::UInt32]
            }
            Select => {
                expect_arity(self.name(), arg_types, 2)?;
                vec![DataType::Binary, DataType::UInt64]
            }
            FromRange => {
                expect_arity(self.name(), arg_types, 2)?;
                vec![DataType::UInt32, DataType::UInt64]
            }
            ContainsRange | RangeCardinality | InsertRange | RemoveRange => {
                expect_arity(self.name(), arg_types, 3)?;
                vec![DataType::Binary, DataType::UInt32, DataType::UInt64]
            }
            FromList => {
                expect_arity(self.name(), arg_types, 1)?;
                vec![DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::UInt32,
                    true,
                )))]
            }
            Union
            | Intersection
            | Difference
            | Xor
            | UnionCardinality
            | IntersectionCardinality
            | DifferenceCardinality
            | XorCardinality
            | Intersects
            | IsDisjoint
            | IsSubset
            | IsSuperset => {
                expect_arity(self.name(), arg_types, 2)?;
                vec![DataType::Binary, DataType::Binary]
            }
        };
        Ok(types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use ScalarKind::*;
        match self.kind {
            Empty => bitmap_output(args.number_rows, |_| Ok(RoaringBitmap::new())),
            Cardinality => unary_u64(args, |bitmap| Ok(bitmap.len())),
            Min => unary_u32(args, |bitmap| Ok(bitmap.min())),
            Max => unary_u32(args, |bitmap| Ok(bitmap.max())),
            IsEmpty => unary_bool(args, |bitmap| Ok(bitmap.is_empty())),
            SerializedSize => unary_u64(args, |bitmap| Ok(bitmap.serialized_size() as u64 + 6)),
            ToList => to_list(args),
            IsValid => is_valid(args),
            Validate => unary_bitmap(args, Ok),
            Contains => bitmap_u32_bool(args, |bitmap, value| bitmap.contains(value)),
            Rank => bitmap_u32_u64(args, |bitmap, value| bitmap.rank(value)),
            Select => select(args),
            FromRange => from_range(args),
            ContainsRange => bitmap_range_bool(args, |bitmap, start, end| {
                Ok(bitmap.range_cardinality(bounds(start, end)?) == end - u64::from(start))
            }),
            RangeCardinality => bitmap_range_u64(args, |bitmap, start, end| {
                Ok(bitmap.range_cardinality(bounds(start, end)?))
            }),
            InsertRange => mutate_range(args, |bitmap, range| {
                bitmap.insert_range(range);
            }),
            RemoveRange => mutate_range(args, |bitmap, range| {
                bitmap.remove_range(range);
            }),
            FromList => from_list(args),
            Insert => mutate_value(args, |bitmap, value| {
                bitmap.insert(value);
            }),
            Remove => mutate_value(args, |bitmap, value| {
                bitmap.remove(value);
            }),
            Union => binary_bitmap(args, |left, right| left | right),
            Intersection => binary_bitmap(args, |left, right| left & right),
            Difference => binary_bitmap(args, |left, right| left - right),
            Xor => binary_bitmap(args, |left, right| left ^ right),
            UnionCardinality => binary_u64(args, |left, right| (left | right).len()),
            IntersectionCardinality => binary_u64(args, |left, right| (left & right).len()),
            DifferenceCardinality => binary_u64(args, |left, right| (left - right).len()),
            XorCardinality => binary_u64(args, |left, right| (left ^ right).len()),
            Intersects => binary_bool(args, |left, right| !left.is_disjoint(right)),
            IsDisjoint => binary_bool(args, RoaringBitmap::is_disjoint),
            IsSubset => binary_bool(args, RoaringBitmap::is_subset),
            IsSuperset => binary_bool(args, |left, right| right.is_subset(left)),
        }
    }
}

fn arrays(args: ScalarFunctionArgs) -> Result<Vec<Arc<dyn Array>>> {
    args.args
        .into_iter()
        .map(|value| value.into_array(args.number_rows))
        .collect()
}

fn bitmaps(array: &Arc<dyn Array>) -> Result<&BinaryArray> {
    array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| datafusion_common::DataFusionError::Internal("expected Binary".to_owned()))
}

fn bitmap_output<F>(rows: usize, mut make: F) -> Result<ColumnarValue>
where
    F: FnMut(usize) -> Result<RoaringBitmap>,
{
    let mut output = BinaryBuilder::new();
    for row in 0..rows {
        output.append_value(encode_bitmap(&make(row)?)?);
    }
    Ok(ColumnarValue::Array(Arc::new(output.finish())))
}

fn unary_bitmap<F>(args: ScalarFunctionArgs, mut op: F) -> Result<ColumnarValue>
where
    F: FnMut(RoaringBitmap) -> Result<RoaringBitmap>,
{
    let values = arrays(args)?;
    let input = bitmaps(&values[0])?;
    let mut output = BinaryBuilder::new();
    for value in input.iter() {
        match value {
            Some(value) => output.append_value(encode_bitmap(&op(decode_bitmap(value)?)?)?),
            None => output.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(output.finish())))
}

macro_rules! unary_primitive {
    ($name:ident, $array:ty, $native:ty) => {
        fn $name<F>(args: ScalarFunctionArgs, mut op: F) -> Result<ColumnarValue>
        where
            F: FnMut(&RoaringBitmap) -> Result<$native>,
        {
            let values = arrays(args)?;
            let input = bitmaps(&values[0])?;
            let output = input
                .iter()
                .map(|value| {
                    value
                        .map(|v| decode_bitmap(v).and_then(|b| op(&b)))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ColumnarValue::Array(Arc::new(<$array>::from(output))))
        }
    };
}

unary_primitive!(unary_u64, UInt64Array, u64);
unary_primitive!(unary_bool, BooleanArray, bool);

fn unary_u32<F>(args: ScalarFunctionArgs, mut op: F) -> Result<ColumnarValue>
where
    F: FnMut(&RoaringBitmap) -> Result<Option<u32>>,
{
    let values = arrays(args)?;
    let input = bitmaps(&values[0])?;
    let output = input
        .iter()
        .map(|value| match value {
            Some(value) => op(&decode_bitmap(value)?),
            None => Ok(None),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ColumnarValue::Array(Arc::new(UInt32Array::from(output))))
}

fn bitmap_u32_bool<F>(args: ScalarFunctionArgs, mut op: F) -> Result<ColumnarValue>
where
    F: FnMut(&RoaringBitmap, u32) -> bool,
{
    let values = arrays(args)?;
    let sets = bitmaps(&values[0])?;
    let numbers = values[1].as_any().downcast_ref::<UInt32Array>().unwrap();
    let output = sets
        .iter()
        .zip(numbers.iter())
        .map(|(set, number)| match (set, number) {
            (Some(set), Some(number)) => decode_bitmap(set).map(|set| Some(op(&set, number))),
            _ => Ok(None),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(output))))
}

fn bitmap_u32_u64<F>(args: ScalarFunctionArgs, mut op: F) -> Result<ColumnarValue>
where
    F: FnMut(&RoaringBitmap, u32) -> u64,
{
    let values = arrays(args)?;
    let sets = bitmaps(&values[0])?;
    let numbers = values[1].as_any().downcast_ref::<UInt32Array>().unwrap();
    let output = sets
        .iter()
        .zip(numbers.iter())
        .map(|(set, number)| match (set, number) {
            (Some(set), Some(number)) => decode_bitmap(set).map(|set| Some(op(&set, number))),
            _ => Ok(None),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ColumnarValue::Array(Arc::new(UInt64Array::from(output))))
}

fn select(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let values = arrays(args)?;
    let sets = bitmaps(&values[0])?;
    let indexes = values[1].as_any().downcast_ref::<UInt64Array>().unwrap();
    let output = sets
        .iter()
        .zip(indexes.iter())
        .map(|(set, index)| match (set, index) {
            (Some(set), Some(index)) if index <= u64::from(u32::MAX) => {
                Ok(decode_bitmap(set)?.select(index as u32))
            }
            (Some(_), Some(_)) => Ok(None),
            _ => Ok(None),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ColumnarValue::Array(Arc::new(UInt32Array::from(output))))
}

fn bounds(start: u32, end: u64) -> Result<(Bound<u32>, Bound<u32>)> {
    if end > u64::from(u32::MAX) + 1 {
        return exec_err!("roaring range end {end} exceeds 2^32");
    }
    if end < u64::from(start) {
        return exec_err!("roaring range end {end} is before start {start}");
    }
    let end = if end == u64::from(u32::MAX) + 1 {
        Bound::Unbounded
    } else {
        Bound::Excluded(end as u32)
    };
    Ok((Bound::Included(start), end))
}

fn from_range(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let values = arrays(args)?;
    let starts = values[0].as_any().downcast_ref::<UInt32Array>().unwrap();
    let ends = values[1].as_any().downcast_ref::<UInt64Array>().unwrap();
    let mut output = BinaryBuilder::new();
    for (start, end) in starts.iter().zip(ends.iter()) {
        match (start, end) {
            (Some(start), Some(end)) => {
                let mut bitmap = RoaringBitmap::new();
                bitmap.insert_range(bounds(start, end)?);
                output.append_value(encode_bitmap(&bitmap)?);
            }
            _ => output.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(output.finish())))
}

macro_rules! bitmap_range_primitive {
    ($name:ident, $array:ty, $native:ty) => {
        fn $name<F>(args: ScalarFunctionArgs, mut op: F) -> Result<ColumnarValue>
        where
            F: FnMut(&RoaringBitmap, u32, u64) -> Result<$native>,
        {
            let values = arrays(args)?;
            let sets = bitmaps(&values[0])?;
            let starts = values[1].as_any().downcast_ref::<UInt32Array>().unwrap();
            let ends = values[2].as_any().downcast_ref::<UInt64Array>().unwrap();
            let output = sets
                .iter()
                .zip(starts.iter())
                .zip(ends.iter())
                .map(|((set, start), end)| match (set, start, end) {
                    (Some(set), Some(start), Some(end)) => {
                        Ok(Some(op(&decode_bitmap(set)?, start, end)?))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ColumnarValue::Array(Arc::new(<$array>::from(output))))
        }
    };
}

bitmap_range_primitive!(bitmap_range_bool, BooleanArray, bool);
bitmap_range_primitive!(bitmap_range_u64, UInt64Array, u64);

fn mutate_value<F>(args: ScalarFunctionArgs, mut op: F) -> Result<ColumnarValue>
where
    F: FnMut(&mut RoaringBitmap, u32),
{
    let values = arrays(args)?;
    let sets = bitmaps(&values[0])?;
    let numbers = values[1].as_any().downcast_ref::<UInt32Array>().unwrap();
    let mut output = BinaryBuilder::new();
    for (set, number) in sets.iter().zip(numbers.iter()) {
        match (set, number) {
            (Some(set), Some(number)) => {
                let mut set = decode_bitmap(set)?;
                op(&mut set, number);
                output.append_value(encode_bitmap(&set)?);
            }
            _ => output.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(output.finish())))
}

fn mutate_range<F>(args: ScalarFunctionArgs, mut op: F) -> Result<ColumnarValue>
where
    F: FnMut(&mut RoaringBitmap, (Bound<u32>, Bound<u32>)),
{
    let values = arrays(args)?;
    let sets = bitmaps(&values[0])?;
    let starts = values[1].as_any().downcast_ref::<UInt32Array>().unwrap();
    let ends = values[2].as_any().downcast_ref::<UInt64Array>().unwrap();
    let mut output = BinaryBuilder::new();
    for ((set, start), end) in sets.iter().zip(starts.iter()).zip(ends.iter()) {
        match (set, start, end) {
            (Some(set), Some(start), Some(end)) => {
                let mut set = decode_bitmap(set)?;
                op(&mut set, bounds(start, end)?);
                output.append_value(encode_bitmap(&set)?);
            }
            _ => output.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(output.finish())))
}

fn binary_bitmap<F>(args: ScalarFunctionArgs, mut op: F) -> Result<ColumnarValue>
where
    F: FnMut(&RoaringBitmap, &RoaringBitmap) -> RoaringBitmap,
{
    let values = arrays(args)?;
    let left = bitmaps(&values[0])?;
    let right = bitmaps(&values[1])?;
    let mut output = BinaryBuilder::new();
    for (left, right) in left.iter().zip(right.iter()) {
        match (left, right) {
            (Some(left), Some(right)) => output.append_value(encode_bitmap(&op(
                &decode_bitmap(left)?,
                &decode_bitmap(right)?,
            ))?),
            _ => output.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(output.finish())))
}

macro_rules! binary_primitive {
    ($name:ident, $array:ty, $native:ty) => {
        fn $name<F>(args: ScalarFunctionArgs, mut op: F) -> Result<ColumnarValue>
        where
            F: FnMut(&RoaringBitmap, &RoaringBitmap) -> $native,
        {
            let values = arrays(args)?;
            let left = bitmaps(&values[0])?;
            let right = bitmaps(&values[1])?;
            let output = left
                .iter()
                .zip(right.iter())
                .map(|(left, right)| match (left, right) {
                    (Some(left), Some(right)) => {
                        Ok(Some(op(&decode_bitmap(left)?, &decode_bitmap(right)?)))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(ColumnarValue::Array(Arc::new(<$array>::from(output))))
        }
    };
}

binary_primitive!(binary_u64, UInt64Array, u64);
binary_primitive!(binary_bool, BooleanArray, bool);

fn from_list(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let values = arrays(args)?;
    let lists = values[0]
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal("expected List<UInt32>".to_owned())
        })?;
    let mut output = BinaryBuilder::new();
    for row in 0..lists.len() {
        if lists.is_null(row) {
            output.append_null();
        } else {
            let list = lists.value(row);
            let values = list.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "expected UInt32 list values".to_owned(),
                )
            })?;
            output.append_value(encode_bitmap(&values.iter().flatten().collect())?);
        }
    }
    Ok(ColumnarValue::Array(Arc::new(output.finish())))
}

fn to_list(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let values = arrays(args)?;
    let sets = bitmaps(&values[0])?;
    let mut output = ListBuilder::new(UInt32Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::UInt32,
        false,
    )));
    for set in sets.iter() {
        match set {
            Some(set) => {
                for value in decode_bitmap(set)? {
                    output.values().append_value(value);
                }
                output.append(true);
            }
            None => output.append(false),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(output.finish())))
}

fn is_valid(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let values = arrays(args)?;
    let input = bitmaps(&values[0])?;
    let output = input
        .iter()
        .map(|value| value.map(|value| decode_bitmap(value).is_ok()))
        .collect::<Vec<_>>();
    Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(output))))
}
