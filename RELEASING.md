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

## InodeDB

`libch` depends on `inodedb-client` 0.1 from crates.io, which speaks the protocol of the InodeDB server at `inodedb.rev`: the commit BigHouse pins its `inodedb` submodule to, which the dev stack runs and the preflight checks. To move to a server whose protocol differs, move `inodedb.rev`, BigHouse's submodule and the client's version together.

## Setup on external services

- **crates.io:** `libch` has two Trusted Publishers for GitHub repository `danthegoodman1/libch`, with no environment: one for workflow `release.yml` and one for `bump.yml`. The OIDC claim names the top-level workflow that started the run, and the auto-release path starts in `bump.yml`, which calls `release.yml`. No repository secret is needed: the workflow mints a token for each run through `rust-lang/crates-io-auth-action`. crates.io → `libch` → Settings → Trusted Publishing lists them, as does `GET /api/v1/trusted_publishing/github_configs?crate=libch`.
- **GitHub:**
  - The ruleset "Protect main" requires a pull request with passing `fmt`, `clippy` and both `test` jobs, and refuses deleting `main` or force-pushing to it. Deploy keys alone may push past it.
  - The deploy key "Release workflow (bump.yml)" has write access, and its private key lives only in the Actions secret `RELEASE_DEPLOY_KEY`. `bump.yml` loads it in the step that pushes the `chore(release): vX.Y.Z` commit, after every build has run. The tag goes with `GITHUB_TOKEN`, which the ruleset leaves alone. To replace the key, make a new one, set the secret to its private half, and delete the old deploy key.
  - A pull request from anyone outside the repository's collaborators runs CI only after a maintainer approves the run (Settings → Actions → General, "Require approval for all external contributors").
  - The workflows ask for `contents: write` themselves, so the repository's default read-only token is enough.

## What runs where

- **ci.yml** (pull requests and `main`): `cargo fmt --check`; `clippy` on the workspace and on the library without features; the docs with `-D warnings`; and `scripts/ci-test` on Linux x86_64 and arm64.
- **bump.yml** (push to `main`): decide the bump level → skip unless `libch` is on crates.io → `scripts/ci-test` → escalate the version with `cargo-semver-checks` → `cargo package` → commit, tag and GitHub release → call `release.yml`.
- **release.yml** (tag push, called, or manual): fmt, clippy and `scripts/ci-test` on Linux x86_64 and arm64 → verify the tag matches `crates/libch/Cargo.toml` → skip if the version is already on crates.io → OIDC auth → `cargo publish -p libch`.

`scripts/ci-test` runs the tests that need neither the dev stack nor the fork's binary. The full suite runs on a machine with both (README.md, section Running the tests); run it before a merge that changes what a worker does.
