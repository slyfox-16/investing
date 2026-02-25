# Saturn Dagster Code Location Setup (External Repo)

This repo exposes Dagster `Definitions` at `jobs.defs`.

Use this guide to publish this repo as a gRPC code location that Saturn Dagster can load.

## 1) Build and publish the code-server image

From repo root:

```bash
docker build -f Dockerfile.dagster-code-server -t <registry>/<namespace>/investing-dagster:<tag> .
docker push <registry>/<namespace>/investing-dagster:<tag>
```

The container starts:

```bash
dagster api grpc -m jobs -h 0.0.0.0 -p 4000
```

## 2) Add this image as a Saturn code location

In the Saturn storage repo, update `storage/services/dagster/workspace.yaml` with a `grpc_server` entry similar to:

```yaml
load_from:
  - grpc_server:
      host: investing-dagster
      port: 4000
      location_name: investing
```

And ensure the associated service/pod for `investing-dagster` uses the published image.

## 3) Deploy storage repo changes

Apply the Saturn storage deployment so Dagster control plane reloads workspace code locations.

Then verify in UI:

- `http://saturn-dagster.home`
- Code location `investing` appears healthy
- Job `build_eth_training` is visible

## 4) Optional GraphQL health check

```bash
curl -sS -X POST \
  http://saturn-dagster.home/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ repositoriesOrError { __typename ... on RepositoryConnection { nodes { name location { name } } } } }"}'
```

## 5) Runtime env in Saturn

If this job needs RPC and tracking credentials, inject env vars into the code-location deployment:

- `INFURA_HTTP`
- `MLFLOW_TRACKING_URI`
- `MLFLOW_TRACKING_USERNAME`
- `MLFLOW_TRACKING_PASSWORD`

Prefer secret-backed env injection in Saturn/Kubernetes rather than baking credentials into images.
