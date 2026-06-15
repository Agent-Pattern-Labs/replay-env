# Releasing

Replay Env releases are tag-driven.

## Local Preflight

Run the same checks CI runs:

```bash
cargo fmt --check
cargo check --locked
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo package --locked
cargo publish --dry-run --locked
```

## GitHub Release

1. Update the crate version in `Cargo.toml`.
2. Update install examples that pin a release tag.
3. Commit and push to `main`.
4. Create and push a matching tag:

```bash
git tag vX.Y.Z
git push origin main
git push origin vX.Y.Z
```

The `Release` workflow verifies the crate, builds native binaries for Linux,
macOS Apple Silicon, and Windows, and creates or updates the GitHub release for
the tag. Intel macOS users should use `cargo install`.

## Crates.io

Use crates.io only when the release is meant to become the canonical Cargo
registry install path:

```bash
cargo publish --locked
```

Crates.io publishes are effectively permanent. Run the dry run first and only
publish from a clean checkout after the GitHub release checks pass.
