# Releasing

Releases are automated. `libch` is the one published crate in this workspace: its version lives in `crates/libch/Cargo.toml`, and `libch-testkit` and `libch-demo` stay here. On every push to `main`, `.github/workflows/bump.yml` ("Auto Release") decides a version, tags it, creates a GitHub release, and calls `.github/workflows/release.yml`, which publishes to crates.io over **Trusted Publishing** (OIDC, with no long-lived token secret).

## Day-to-day

Merge to `main`. By default the version bumps by a **patch** and escalates (patch → minor → major) until `cargo-semver-checks` agrees the level covers the public-API diff since the last published release, so a breaking change cannot go out as a patch.

Control it with the head commit message (the PR title, for merge commits and squash merges):

- `#minor` / `#major`: force that bump level (the run fails if the level can't cover the API diff).
- `[skip release]`: release nothing for this push.

**Actions → Auto Release → Run workflow** releases with a level you pick. **Actions → Release → Run workflow** publishes an existing `vX.Y.Z` tag whose publish never landed; it does nothing when that version is already on crates.io.

## The worker

Each release's notes name the commit in `fork.rev`: the fork's `libch-worker` commit that the release speaks to. `cargo-semver-checks` reads only the Rust API, and `libch::proto` stays out of it. So when a merge needs a newer worker than the last release spoke to, put `#minor` in its message: a service then deploys the new worker with the new version.

## Before the first release

These are manual steps on external services, in order. Until the first one is done, Auto Release notes that `libch` is not on crates.io yet and releases nothing.

### 1. InodeDB's client on crates.io

`libch` reaches InodeDB through `inodedb-client`, and crates.io accepts only dependencies that are on crates.io. When InodeDB publishes `inodedb-client` and the crates it depends on, change the `inodedb` entry in the workspace `Cargo.toml` from its `git` and `rev` to that version. `preflight::inodedb_client_rev` reads the client's commit from that entry, so have it name the commit the version was cut from. `cargo package -p libch` then succeeds; until then it stops at that dependency.

### 2. Publish the first version by hand

crates.io sets up Trusted Publishing only for a crate that exists. With a personal token from crates.io → Account Settings → API Tokens:

```sh
cargo login
cargo publish -p libch
git tag v0.1.0 && git push origin v0.1.0
```

The tag runs `release.yml`, which finds the version on crates.io and creates its GitHub release.

### 3. Trusted Publishing

On crates.io → `libch` → Settings → Trusted Publishing, add two GitHub publishers:

- Repository owner `danthegoodman1`, repository `libch`, workflow `release.yml`, no environment.
- The same with workflow `bump.yml`. The OIDC claim names the top-level workflow that started the run, and the auto-release path starts in `bump.yml`, which calls `release.yml`.

No repository secret is needed: the workflow mints a token for each run through `rust-lang/crates-io-auth-action`.

### 4. Repository settings

The workflows ask for `contents: write` themselves, so the repository's default read-only token is enough. If `main` gets branch protection, let `github-actions[bot]` push the `chore(release): vX.Y.Z` commit and the tag.

## What runs where

- **ci.yml** (pull requests and `main`): `cargo fmt --check`; `clippy` on the workspace and on the library without features; the docs with `-D warnings`; and `scripts/ci-test` on Linux x86_64 and arm64.
- **bump.yml** (push to `main`): decide the bump level → skip unless `libch` is on crates.io → `scripts/ci-test` → escalate the version with `cargo-semver-checks` → `cargo package` → commit, tag and GitHub release → call `release.yml`.
- **release.yml** (tag push, called, or manual): fmt, clippy and `scripts/ci-test` on Linux x86_64 and arm64 → verify the tag matches `crates/libch/Cargo.toml` → skip if the version is already on crates.io → OIDC auth → `cargo publish -p libch`.

`scripts/ci-test` runs the tests that need neither the dev stack nor the fork's binary. The full suite runs on a machine with both (README.md, section Running the tests); run it before a merge that changes what a worker does.
