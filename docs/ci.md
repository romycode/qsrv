# GitHub Actions

What each workflow does, when it runs, what it protects you from, and what
to do when it fails. Files are in `.github/workflows/`.

## At a glance

| workflow | runs on | for |
|---|---|---|
| [Go](#go-goyml) | PRs and `main`, when Go code changes | the server is correct, durable and not slower |
| [PHP](#php-phpyml) | PRs and `main`, when `php/` or the server changes | the PHP client and Messenger transport work against a real server |
| [Docker](#docker-dockeryml) | PRs and `main` when the image's inputs change; release tags | the image builds for amd64 and arm64 and works; publishes it |
| [Commits](#commits-commitsyml) | every PR | commit messages and the PR title follow Conventional Commits |
| [Release](#release-releaseyml) | `vX.Y.Z` tags | binaries and the GitHub release |
| [Tag release](#tag-release-tagyml) | by hand, from the Actions tab | creates a release (server or PHP) with one click |
| [Monitoring](#monitoring-monitoringyml) | PRs and `main`, when the alert rules, dashboards or metrics change | the alerts and Grafana dashboards work against a real qsrv |

The first four guard changes; the last two publish them.

**Each workflow only runs when its files change** (GitHub `paths` filters).
A docs-only PR runs only Commits; a PHP-only change doesn't run the Go
tests. Changing a workflow file runs that workflow. PHP also runs when the
server changes, because its integration tests start a real qsrv.

**A new push cancels the previous run of the same workflow on the same PR**
(`concurrency`), so a "cancelled" check on an old commit is normal. Look at
the checks of the latest commit.

## Go (`go.yml`)

The main gate. Four jobs in parallel, then one that posts the results.

| job | checks | fails when |
|---|---|---|
| **Lint & build** | `gofmt`, `go vet` with both SQLite drivers, `go mod verify`, static builds for linux/amd64, linux/arm64, darwin/arm64 | code not formatted, vet warnings, a platform doesn't compile |
| **Tests** (WASM, cgo) | `go test -race ./...` once per SQLite driver; coverage with the WASM driver, compared with the PR's base | any test fails or a data race is found |
| **Crash test** (WASM, cgo) | builds the real `qsrv` and kills it with SIGKILL 20 times under load (two tenants, a topic, producers and consumers), then drains everything | a message answered `201` is lost, or a message whose ack was answered `204` is delivered again. These are qsrv's two promises: if this fails, don't merge |
| **Performance** | Go benchmarks of the base and the PR, interleaved 6 times, compared with `benchstat`; a `qbench` load test over HTTP | never fails the build on numbers (shared runners are noisy); fails only if the benchmarks don't run |
| **PR report** | gathers the results into one PR comment | — |

Run the same locally:

```sh
gofmt -l . && go vet ./... && go vet -tags sqlite_cgo ./...
go test -race ./...                                   # WASM driver
go test -race -tags sqlite_cgo ./...                  # cgo driver
go test -tags e2e -run Crash -v ./internal/e2e/       # crash test
QSRV_E2E_TAGS=sqlite_cgo go test -tags e2e -run Crash ./internal/e2e/
```

Reading the performance section of the PR comment:
[.github/ci/bench-guide.md](../.github/ci/bench-guide.md) (it is also in the
comment). In short: only changes above ~10% that `benchstat` marks with a
confidence interval mean something.

## PHP (`php.yml`)

PHPUnit for `romycode/qsrv-php`, unit and integration tests, in a matrix
of PHP 8.4 and 8.5 × Symfony 7.4 and 8.1 (four jobs).

- Builds and starts a real `qsrv` on the runner, and the integration tests
  run against it (client, Messenger transport, retries with a real Worker).
- Runs with `--fail-on-skipped`: an integration test that is skipped (for
  example because the server didn't start) fails the job instead of passing
  silently.
- `composer validate` checks `php/composer.json`.
- Posts one PR comment with PHPUnit's result per combination; on failure,
  the last lines of its output.

Locally: see [php/README.md](../php/README.md#tests).

## Monitoring (`monitoring.yml`)

Checks the alerts and the Grafana dashboards against a live stack, because
a dashboard that loads can still query a metric or label that doesn't exist.

1. `promtool check rules` and `check config`, `amtool check-config`.
2. Starts `deploy/monitoring` with qsrv built from the PR, and generates
   load that produces every kind of event: sends, acks, ordering groups,
   delays, nacks, dead letters, lease expiries and a full queue.
3. Runs every query of every panel and variable through Grafana's API.
   **Fails** if Grafana or Prometheus rejects one; lists the panels without
   data (expected for what the load doesn't produce, such as HTTP 500s).
4. Takes screenshots of the three dashboards, light and dark, and uploads
   them as the `grafana-dashboards` artifact of the run.

Runs when `deploy/prometheus/`, `deploy/grafana/`, `deploy/monitoring/` or
the metrics code (`internal/obs/`) change.

## Docker (`docker.yml`)

Builds the image `ghcr.io/romycode/qsrv`: `qsrv` with native SQLite (cgo,
linked statically against musl, cross-compiled without QEMU) and `qctl`, on
`distroless/static`.

1. Builds the amd64 image and **smoke-tests** it: it must report healthy,
   print a version with the cgo driver, and do a full send → receive → ack
   with `qctl` inside the container.
2. Builds both platforms (amd64, arm64).
3. Pushes, except on PRs:

| event | image tags |
|---|---|
| PR | nothing pushed (build and smoke test only) |
| push to `main` | `edge`, `sha-<commit>` |
| tag `v0.1.0` | `0.1.0`, `0.1`, `latest` (and `0` from 1.0) |

It uses the workflow's own `GITHUB_TOKEN` to push: no secrets. A newly
published package is private; make it public once in its GitHub settings.

## Commits (`commits.yml`)

Checks [Conventional Commits](https://www.conventionalcommits.org) on
every commit of the PR and on the PR title (the title is the commit subject
when the PR is squash-merged). Runs again when you edit the title.

Fix a failure by rewording the commits (`git rebase -i`, then force-push) or
the PR title. Same check locally, on every commit:

```sh
git config core.hooksPath .githooks
scripts/check-commit-msg.sh --subject "feat(queue): something"
```

## Release (`release.yml`)

Runs on a server tag `vX.Y.Z` (PHP tags are `php/vX.Y.Z` and don't trigger
it), or when Tag release dispatches it.

1. Takes the tag's section from `CHANGELOG.md` as release notes. **Fails if
   there is no `## [X.Y.Z]` section**: add it before tagging.
2. Builds pure-Go binaries (SQLite as WASM, no dependencies): `qsrv` and
   `qctl` for Linux and macOS (amd64, arm64), `qctl` for Windows. Each
   package includes the README and CHANGELOG (and LICENSE, once the
   repository has one).
3. Writes `checksums.txt` (SHA-256) and smoke-tests the linux/amd64 binary:
   `qsrv -version` must print the tag's version.
4. Creates the GitHub release with the packages and the checksums.

The Docker image for the same tag comes from `docker.yml`.

## Tag release (`tag.yml`)

The only workflow you start by hand: **Actions → Tag release → Run
workflow**, on `main`, choosing a component and a version.

**`server`, `v0.1.0`**
1. Checks the version format, that the tag doesn't exist yet, and that
   `CHANGELOG.md` has a `## [0.1.0]` section.
2. Tags the current `main` as `v0.1.0` and pushes the tag.
3. Starts `release.yml` and `docker.yml` on that tag. (A tag pushed by an
   action doesn't trigger other workflows by itself; starting them
   explicitly does.)

**`php`, `v0.1.0`**
1. Runs `scripts/release-php.sh v0.1.0`: extracts `php/`, pushes it to the
   read-only mirror `romycode/qsrv-php` (refusing to overwrite it), tags
   `php/v0.1.0` here and `v0.1.0` on the mirror.
2. Packagist sees the mirror's tag and publishes the version (through the
   webhook that `setup-release.sh` adds).

Needs the secret `QSRV_PHP_MIRROR_KEY`; without it, the job stops at the
first step and says so.

Pushing a tag yourself (`git push origin v0.1.0`) does the same for the
server, and `scripts/release-php.sh` from your machine does the same for
PHP.

### Release flow

```
CHANGELOG.md: move [Unreleased] to [0.1.0] (PR, merge)
        │
Actions → Tag release (server, v0.1.0)
        │  tag v0.1.0 on main
        ├──► release.yml ──► GitHub release: binaries + checksums + notes
        └──► docker.yml  ──► ghcr.io/romycode/qsrv:0.1.0, 0.1, latest

Actions → Tag release (php, v0.1.0)
        │  php/ → mirror romycode/qsrv-php, tag v0.1.0 there
        └──► Packagist (webhook) ──► composer require romycode/qsrv-php:^0.1
```

## One-time setup

`scripts/setup-release.sh` prepares everything releases need, with the `gh`
CLI (run it as an admin of both repositories; it is safe to re-run):

```sh
PACKAGIST_USERNAME=you PACKAGIST_TOKEN=... scripts/setup-release.sh
```

- squash-only merges and deleting branches after merge, on `romycode/qsrv`;
- a deploy key with write access on the mirror, stored as the
  `QSRV_PHP_MIRROR_KEY` secret (`--rotate-key` replaces it);
- with the Packagist variables: registers the package and adds the
  webhook that publishes new tags.

## Permissions and secrets

Every workflow starts from read-only permissions and asks only for what it
needs:

| workflow | writes | with |
|---|---|---|
| Go, PHP | one PR comment each | `GITHUB_TOKEN` (`pull-requests: write`, report job only) |
| Docker | the image in GHCR | `GITHUB_TOKEN` (`packages: write`) |
| Release | the GitHub release | `GITHUB_TOKEN` (`contents: write`) |
| Tag release, server | the tag; starts two workflows | `GITHUB_TOKEN` (`contents: write`, `actions: write`) |
| Tag release, php | the tag here; the mirror | `GITHUB_TOKEN`; secret `QSRV_PHP_MIRROR_KEY` |

`QSRV_PHP_MIRROR_KEY` is the only secret. PRs from forks run the checks but
don't post comments (their token can't write to the PR).

## Shared pieces

| file | what |
|---|---|
| `.github/actions/pr-comment` | posts one comment per workflow on the PR and updates it on every run, instead of adding a new one each time |
| `.github/ci/report` | small Go program that turns `go test -json` and coverage profiles into the Markdown of the PR comment |
| `.github/ci/bench-guide.md` | how to read the benchmark results (included in the comment) |
| `scripts/check-commit-msg.sh` | the Conventional Commits check (CI and the git hook) |
| `scripts/release-php.sh` | publishes `php/` to the mirror |
| `scripts/setup-release.sh` | one-time release setup |

## When something goes wrong

- **A check never appears on my PR.** Its path filter didn't match: the PR
  doesn't touch those files, so the workflow didn't run. That is expected.
  For the same reason, if you make checks *required* in branch protection,
  require only Commits, or use a ruleset that skips checks that didn't run:
  otherwise a docs PR waits forever for a Go check.
- **A check is "cancelled".** A newer push replaced that run. Look at the
  latest commit's checks.
- **Crash test failed.** Real problem: a confirmed message was lost or an
  acked one came back. The log shows which messages and the qsrv log. Run
  it locally with more rounds (`QSRV_E2E_ROUNDS=50`).
- **Performance looks worse.** Check `benchstat`'s confidence interval and
  re-run the job: one noisy runner is common. A real regression repeats.
- **PHP integration tests failed but Go is green.** The failing step prints
  the qsrv log; the change usually broke the HTTP API contract the client
  relies on.
- **Tag release (server) says there is no CHANGELOG section.** Add
  `## [X.Y.Z] - date` to `CHANGELOG.md` in a PR, merge, run it again.
- **Tag release (php) says the secret is missing.** Run
  `scripts/setup-release.sh` once.
- **The image can't be pulled** (`unauthorized`): the GHCR package is still
  private; change its visibility in the package settings.
