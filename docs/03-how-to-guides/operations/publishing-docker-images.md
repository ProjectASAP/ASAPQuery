# Publishing Docker Images to GHCR

Docker images are published to `ghcr.io/projectasap/` by pushing a version tag. Nothing is published on regular commits or PRs.

## How to publish a release

```bash
git tag v0.2.0
git push origin v0.2.0
```

That's it. GitHub Actions will build and push all images tagged as both `v0.2.0` and `latest`.

## Images published from this repo

| Image | Source |
|---|---|
| `ghcr.io/projectasap/asap-base` | `asap-common/installation/` |
| `ghcr.io/projectasap/asap-planner-rs` | `asap-planner-rs/` |
| `ghcr.io/projectasap/asap-summary-ingest` | `asap-summary-ingest/` |
| `ghcr.io/projectasap/asap-query-engine` | `asap-query-engine/` |
| `ghcr.io/projectasap/asap-prometheus-client` | `asap-tools/queriers/prometheus-client/` |

The arroyo image (`ghcr.io/projectasap/asap-arroyo`) is published separately from the [ProjectASAP/arroyo](https://github.com/ProjectASAP/arroyo) repo using the same process.

## Coordinating with the arroyo repo

The quickstart docker-compose pins specific image versions. When cutting a release, tag both repos with the same version and update the quickstart compose file to reference the new tags.

```bash
# In this repo
git tag v0.2.0 && git push origin v0.2.0

# In ProjectASAP/arroyo
git tag v0.2.0 && git push origin v0.2.0
```

Then update the image tags in `asap-quickstart/` to `v0.2.0`.

## If you need to delete a bad tag

```bash
git tag -d v0.2.0
git push origin :refs/tags/v0.2.0
```

Then delete the package version manually in the GitHub UI under `github.com/orgs/ProjectASAP/packages` if images were already pushed.
