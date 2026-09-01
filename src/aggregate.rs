use std::sync::{Arc, LazyLock};

use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{Array, ArrayRef, BinaryArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use roaring::RoaringBitmap;

use crate::codec::{decode_bitmap, encode_bitmap};

static VALUE_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::uniform(1, vec![DataType::UInt32], Volatility::Immutable));
static BITMAP_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::uniform(1, vec![DataType::Binary], Volatility::Immutable));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum AggregateKind {
    Values,
    Union,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RoaringAggregate {
    name: &'static str,
    kind: AggregateKind,
}

impl RoaringAggregate {
    pub(crate) const fn new(name: &'static str, kind: AggregateKind) -> Self {
        Self { name, kind }
    }
}

#[derive(Debug)]
struct BitmapAccumulator {
    bitmap: RoaringBitmap,
    input: AggregateKind,
}

impl Accumulator for BitmapAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 1 {
            return exec_err!("roaring aggregate expects one argument");
        }
        match self.input {
            AggregateKind::Values => {
                for value in values[0].as_primitive::<UInt32Type>().iter().flatten() {
                    self.bitmap.insert(value);
                }
            }
            AggregateKind::Union => {
                let input = values[0]
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "roaring aggregate input must be Binary".to_owned(),
                        )
                    })?;
                for bytes in input.iter().flatten() {
                    self.bitmap |= decode_bitmap(bytes)?;
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(encode_bitmap(&self.bitmap)?)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.bitmap.serialized_size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 1 {
            return exec_err!("roaring aggregate expects one state field");
        }
        let states = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("roaring aggregate state must be Binary".to_owned())
            })?;
        for bytes in states.iter().flatten() {
            self.bitmap |= decode_bitmap(bytes)?;
        }
        Ok(())
    }
}

impl AggregateUDFImpl for RoaringAggregate {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        match self.kind {
            AggregateKind::Values => &VALUE_SIGNATURE,
            AggregateKind::Union => &BITMAP_SIGNATURE,
        }
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn return_field(&self, _: &[FieldRef]) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, false)))
    }

    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(BitmapAccumulator {
            bitmap: RoaringBitmap::new(),
            input: self.kind,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}_state", args.name),
            DataType::Binary,
            false,
        ))])
    }
}
