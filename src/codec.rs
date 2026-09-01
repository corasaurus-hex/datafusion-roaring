//! Versioned serialization for roaring values.

use std::io::{self, Cursor};

use datafusion_common::{DataFusionError, Result};
use roaring::RoaringBitmap;

const MAGIC: &[u8; 4] = b"DFRB";
const VERSION: u8 = 1;
const BITMAP_KIND: u8 = 1;
const HEADER_LEN: usize = 6;

fn codec_error(action: &str, message: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!("could not {action} roaring value: {message}"))
}

fn payload<'a>(bytes: &'a [u8], expected_kind: u8, type_name: &str) -> Result<&'a [u8]> {
    if bytes.len() < HEADER_LEN {
        return Err(codec_error(
            "decode",
            "value is shorter than the format header",
        ));
    }
    if &bytes[..4] != MAGIC {
        return Err(codec_error("decode", "invalid format magic"));
    }
    if bytes[4] != VERSION {
        return Err(codec_error(
            "decode",
            format!("unsupported format version {}", bytes[4]),
        ));
    }
    if bytes[5] != expected_kind {
        return Err(codec_error("decode", format!("value is not a {type_name}")));
    }
    Ok(&bytes[HEADER_LEN..])
}

fn encode_header(output: &mut Vec<u8>, kind: u8) {
    output.extend_from_slice(MAGIC);
    output.push(VERSION);
    output.push(kind);
}

/// Serializes a 32-bit roaring bitmap using this crate's versioned format.
pub fn encode_bitmap(bitmap: &RoaringBitmap) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(HEADER_LEN + bitmap.serialized_size());
    encode_header(&mut output, BITMAP_KIND);
    bitmap
        .serialize_into(&mut output)
        .map_err(|error| codec_error("encode UInt32", error))?;
    Ok(output)
}

/// Deserializes a 32-bit roaring bitmap encoded by [`encode_bitmap`].
pub fn decode_bitmap(bytes: &[u8]) -> Result<RoaringBitmap> {
    let payload = payload(bytes, BITMAP_KIND, "UInt32 bitmap")?;
    let mut input = Cursor::new(payload);
    let bitmap = RoaringBitmap::deserialize_from(&mut input)
        .map_err(|error: io::Error| codec_error("decode UInt32", error))?;
    if input.position() != payload.len() as u64 {
        return Err(codec_error("decode", "trailing bytes after bitmap payload"));
    }
    Ok(bitmap)
}
