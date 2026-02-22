# Releasing Auphanim

This document explains how to publish a new release and get it onto Homebrew.
Everything is automated once the one-time setup is done.

---

## How it works

The project uses [GoReleaser](https://goreleaser.com) to handle releases.
When you push a version tag to GitHub, a GitHub Actions workflow
(`.github/workflows/release.yml`) runs GoReleaser automatically. It:

1. Cross-compiles the binary for macOS (arm64 + amd64), Linux (arm64 + amd64),
   and Windows (amd64).
2. Creates a GitHub Release with downloadable tarballs and a `checksums.txt`.
3. Generates a Homebrew formula and pushes it to the
   `brucevanhorn2/homebrew-tap` repository.

Users can then install via `brew install brucevanhorn2/tap/auphanim` or
download a binary directly from the Releases page.

Normal pushes to `main` **do not** trigger a release — only a version tag does.

---

## One-time setup (do this before your first release)

These steps only need to be done once. If you are returning to this project
after a break, check each item off before tagging.

### 1. Create the Homebrew tap repository

On GitHub, create a new **public** repository named exactly:

```
homebrew-tap
```

Full URL: `https://github.com/brucevanhorn2/homebrew-tap`

It can be completely empty — GoReleaser will create and push the formula file
automatically when you release. Do not add a README or any files; just create
the repo.

### 2. Create a Personal Access Token (PAT)

GoReleaser needs permission to push to `homebrew-tap` on your behalf.

1. Go to **GitHub → Settings → Developer settings → Personal access tokens →
   Fine-grained tokens**.
2. Click **Generate new token**.
3. Set the token name to something memorable, e.g. `auphanim-goreleaser`.
4. Set **Resource owner** to `brucevanhorn2`.
5. Under **Repository access**, choose **Only select repositories** and pick
   `homebrew-tap`.
6. Under **Repository permissions**, set **Contents** to **Read and write**.
7. Click **Generate token** and **copy the token value immediately** — you
   cannot see it again.

### 3. Add the token as a repository secret

The release workflow reads the token from a secret named `TAP_GITHUB_TOKEN`.

1. Go to the `auphanim` repository on GitHub.
2. Click **Settings → Secrets and variables → Actions**.
3. Click **New repository secret**.
4. Name: `TAP_GITHUB_TOKEN`
5. Value: paste the token you copied in the previous step.
6. Click **Add secret**.

> `GITHUB_TOKEN` (used to create the Release itself) is provided automatically
> by GitHub Actions — you do not need to create it.

### 4. Verify the GoReleaser config locally (optional but recommended)

Install GoReleaser once on your machine:

```bash
brew install goreleaser
# or: go install github.com/goreleaser/goreleaser/v2@latest
```

Then do a dry-run snapshot build to confirm everything compiles for all
platforms before you tag:

```bash
goreleaser build --snapshot --clean
# Binaries appear in dist/ — nothing is published
```

---

## Releasing a new version

Once the one-time setup is complete, every future release is just two commands.

### Step 1 — Make sure main is ready

```bash
git status          # should be clean
git log --oneline   # confirm the commit you want to tag is at the top
```

Run the tests one more time if you want peace of mind:

```bash
go test ./...
```

### Step 2 — Choose a version number

Auphanim follows [Semantic Versioning](https://semver.org):

| Change | Example | When to use |
|---|---|---|
| Patch | `v0.1.0` → `v0.1.1` | Bug fix, no new features |
| Minor | `v0.1.0` → `v0.2.0` | New watcher type or feature, backwards compatible |
| Major | `v0.x.x` → `v1.0.0` | Breaking config/API change |

### Step 3 — Tag and push

```bash
git tag v0.2.0
git push origin v0.2.0
```

That's it. The release workflow starts within seconds. You can watch it at:

```
https://github.com/brucevanhorn2/auphanim/actions
```

After it finishes (usually 2–3 minutes) you will see:

- A new entry on the **Releases** page with tarballs and `checksums.txt`.
- A new or updated `auphanim.rb` formula committed to `homebrew-tap`.

### Step 4 — Verify the Homebrew formula (optional)

```bash
brew update
brew install brucevanhorn2/tap/auphanim
auphanim --version
```

---

## Fixing a bad release

If the release workflow fails or you tagged the wrong commit:

**Delete the tag and re-run:**

```bash
# Delete locally
git tag -d v0.2.0

# Delete on GitHub
git push origin --delete v0.2.0

# Fix whatever was wrong, then re-tag
git tag v0.2.0
git push origin v0.2.0
```

If the GitHub Release was partially created, delete it manually on the Releases
page before re-pushing the tag, otherwise GoReleaser will refuse to overwrite it.

**Never re-use a tag that was already successfully released and downloaded by
users** — their Homebrew installs will have cached the old checksums and the
new binaries will fail verification. Issue a patch version instead (`v0.2.1`).

---

## What lives where

| File | Purpose |
|---|---|
| `.goreleaser.yaml` | GoReleaser config: build targets, archive format, Homebrew formula settings |
| `.github/workflows/release.yml` | GitHub Actions workflow that runs GoReleaser on tag push |
| `.github/workflows/test.yml` | CI workflow that runs tests on every push/PR (no release) |
| `brucevanhorn2/homebrew-tap` | Separate GitHub repo; GoReleaser pushes `auphanim.rb` here |

---

## Updating the formula manually (if ever needed)

You should never need to touch the formula by hand — GoReleaser regenerates it
on every release. But if something goes wrong and you need to edit it directly,
the file is at:

```
https://github.com/brucevanhorn2/homebrew-tap/blob/main/Formula/auphanim.rb
```

The two fields you would ever change manually are `url` (the tarball URL) and
`sha256` (the checksum from `checksums.txt` on the Releases page).
