# Repository guidance

## What this crate does

`datafusion-roaring` lets Apache DataFusion SQL queries build and operate on compressed sets of `UInt32` values. DataFusion is a Rust query engine, and a Roaring bitmap is a compact set of unsigned 32-bit integers.

The data flow is:

```text
UInt32 rows --roaring_agg--> Binary bitmap --scalar functions--> bitmap, count, list, or Boolean
Binary rows --roaring_or_agg-----------------> Binary bitmap
```

A scalar user-defined function (UDF) transforms its arguments row by row. An aggregate user-defined function (UDAF) combines values from many rows. The complete [registration inventory](src/lib.rs) contains 40 scalar SQL names and three aggregate SQL names. Aliases account for several of those names.

Bitmap members are `UInt32`. Encoded bitmaps use Arrow `Binary`, DataFusion's byte-string column type. Cardinality means the number of members and uses `UInt64`. Ranges are half-open: `start` is included and `end` is excluded. The `UInt64` end value `4294967296` represents the boundary after `UInt32::MAX`.

## Using the crate

A DataFusion `SessionContext` owns registered SQL functions and query state. Follow the [README installation and registration example](README.md) to register the full inventory with `all_udfs()` and `all_udafs()`. Callers that need only selected functions can register one of the individual constructors exported from [`src/lib.rs`](src/lib.rs). This crate must not create or mutate a caller's `SessionContext`.

## Layout

| Path                                   | Responsibility                                                                                     |
| -------------------------------------- | -------------------------------------------------------------------------------------------------- |
| [`Cargo.toml`](Cargo.toml)             | Package metadata, Rust version, and the aligned DataFusion/Arrow dependency set.                   |
| [`.github/workflows/ci.yml`](.github/workflows/ci.yml) | Pull request checks and the reusable release verification gate.                                   |
| [`.github/workflows/release-plz.yml`](.github/workflows/release-plz.yml) | crates.io publishing, GitHub Releases, and release pull requests.                    |
| [`README.md`](README.md)               | Installation, registration, SQL inventory, types, null behavior, and feature scope.                |
| [`src/lib.rs`](src/lib.rs)             | Public constructors and the complete registration inventories.                                     |
| [`src/scalar.rs`](src/scalar.rs)       | Scalar function kinds, DataFusion type coercion, return types, and execution.                      |
| [`src/aggregate.rs`](src/aggregate.rs) | Value aggregation, bitmap union aggregation, and accumulator state shared across input partitions. |
| [`src/codec.rs`](src/codec.rs)         | The persistent `DFRB` byte envelope and Roaring payload encoding.                                  |
| [`tests/api.rs`](tests/api.rs)         | Public function names and constructor coverage.                                                    |
| [`tests/codec.rs`](tests/codec.rs)     | Serialization round trips and malformed input rejection.                                           |
| [`tests/sql.rs`](tests/sql.rs)         | SQL planning, coercion, aliases, nulls, ranges, aggregation, and end-to-end results.               |

## Persistent encoding

Encoded bitmap bytes may be stored outside the process, so treat their layout as a compatibility boundary.

| Bytes  | Content                                          |
| ------ | ------------------------------------------------ |
| `0..4` | Magic bytes `DFRB`.                              |
| `4`    | Format version.                                  |
| `5`    | Value kind. `1` identifies a 32-bit bitmap.      |
| `6..`  | Portable payload written by the `roaring` crate. |

Decoding rejects unknown headers, truncated payloads, and trailing bytes. An incompatible layout or payload interpretation requires a new format version, tests for old and new bytes, and a documented migration policy.

## Making changes

### Scalar functions

When adding or changing a scalar function:

1. Update `ScalarKind`, `output_type`, `coerce_types`, and `invoke_with_args` in `src/scalar.rs`.
2. Add its public registration entry in `src/lib.rs`, then update the inventory test and README table.
3. Cover its SQL result, ordinary SQL literal coercion, null behavior, malformed input behavior, and relevant numeric boundaries.

Keep coercion explicit because DataFusion SQL integer literals commonly begin as `Int64`. Tests must prove those literals reach the required `UInt32` or `UInt64` types.

### Aggregates

When adding or changing an aggregate:

1. Update `AggregateKind`, its signature, accumulator update behavior, and accumulator merge behavior in `src/aggregate.rs`.
2. Add its public registration entry in `src/lib.rs`, then update the inventory test and README table.
3. Test duplicates, null-only input, multiple input partitions, and malformed encoded states when applicable.

Multiple partitions matter because DataFusion builds partial accumulator states and combines them through `merge_batch`. A test with one `RecordBatch`, DataFusion's in-memory table batch type, does not exercise that path.

### Codec

For a codec change, add fixed byte-level cases to `tests/codec.rs` before changing `src/codec.rs`. Preserve strict full-payload consumption and distinguish malformed data from unsupported versions in returned errors.

### Dependencies and style

Upgrade DataFusion and Arrow as one compatible set. Use `cargo info datafusion@VERSION` to read DataFusion's minimum supported Rust version, update `rust-version`, then update the lockfile. Expect UDF trait changes across DataFusion releases. Keep `default-features = false` on library dependencies unless the source needs a named feature.

Code comments and doc comments should explain a non-obvious constraint or reason. Remove comments that only restate the code. Keep Markdown paragraphs, bullet items, and table cells on one logical line.

## Verification

Run this release check from the repository root:

```console
cargo fmt --all --check
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps
cargo +1.94.0 check --lib
cargo package --list --allow-dirty
cargo publish --dry-run --allow-dirty
```

Each command must exit with status 0. The dry run succeeds when Cargo verifies the extracted archive and ends with `aborting upload due to dry run`. Inspect the package list and confirm it contains only Cargo's normalized and original manifests, the lockfile, licenses, README, source, and tests.

`--allow-dirty` verifies the working tree during development. Before a real release, commit the intended files, require an empty `git status --short`, and repeat the package list and publish dry run without `--allow-dirty`.

## Publishing

`release-plz` owns version bumps, changelog updates, crates.io publication, tags, and GitHub Releases. Its release pull request is the place to review the next version and release notes. Merging that pull request triggers publication only after the full release check passes.

Before enabling the workflow, set a repository URL in `Cargo.toml`, add a `CARGO_REGISTRY_TOKEN` repository secret with permission to publish this crate, and enable GitHub Actions to create pull requests in the repository settings. The initial `main` push publishes version `0.1.0` because it is not yet present on crates.io. After the first publication, crates.io trusted publishing can replace the long-lived token; remove `CARGO_REGISTRY_TOKEN` from the workflow and grant the release job `id-token: write` at the same time.

Docs.rs automatically builds the API site after crates.io accepts a release. Verify the crate metadata on crates.io, the docs.rs build status, the GitHub tag, and the GitHub Release before considering a release complete.

Do not commit, push, tag, or run a real `cargo publish` unless the user explicitly asks for that action.
