use datafusion_roaring::{decode_bitmap, encode_bitmap};
use roaring::RoaringBitmap;

#[test]
fn bitmap_codec_round_trips() {
    let bitmap = RoaringBitmap::from_iter([1, 7, u32::MAX]);
    let bytes = encode_bitmap(&bitmap).unwrap();
    assert_eq!(decode_bitmap(&bytes).unwrap(), bitmap);
}

#[test]
fn bitmap_codec_v1_bytes_are_stable() {
    let bitmap = RoaringBitmap::from_iter([1, 7, 65_536]);
    let expected = [
        68, 70, 82, 66, 1, 1, 58, 48, 0, 0, 2, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 24, 0, 0, 0, 28, 0,
        0, 0, 1, 0, 7, 0, 0, 0,
    ];
    let bytes = encode_bitmap(&bitmap).unwrap();
    assert_eq!(bytes, expected);
    assert_eq!(decode_bitmap(&expected).unwrap(), bitmap);
}

#[test]
fn bitmap_codec_rejects_unversioned_and_truncated_values() {
    assert!(decode_bitmap(&[]).is_err());
    assert!(decode_bitmap(b"not a roaring bitmap").is_err());

    let encoded = encode_bitmap(&RoaringBitmap::from_iter([1, 7, u32::MAX])).unwrap();
    let mut truncated = encoded.clone();
    truncated.pop();
    assert!(decode_bitmap(&truncated).is_err());
}

#[test]
fn bitmap_codec_rejects_unknown_headers_and_trailing_bytes() {
    let encoded = encode_bitmap(&RoaringBitmap::from_iter([1, 7, u32::MAX])).unwrap();

    for index in [0, 4, 5] {
        let mut changed = encoded.clone();
        changed[index] = changed[index].wrapping_add(1);
        assert!(decode_bitmap(&changed).is_err());
    }

    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode_bitmap(&trailing).is_err());
}
