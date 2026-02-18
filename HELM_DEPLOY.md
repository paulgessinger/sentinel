# Deploying Sentinel to OpenShift with Helm

## Prerequisites

- OpenShift cluster with `oc` CLI configured and logged in
- Helm 3 installed (`brew install helm`)
- A GitHub App with a private key, app ID, and webhook secret
- The Sentinel container image pushed to a registry accessible from the cluster
  (default: `ghcr.io/paulgessinger/sentinel`)

---

## Architecture overview

```
GitHub webhook
      │
      ▼
┌─────────────┐          ┌─────────────────────────┐
│  Web server │──write──▶│  PVC (/data)            │
│  (uvicorn)  │          │  • diskcache            │
└─────────────┘          │  • webhooks.sqlite3     │
                         └─────────────────────────┘
```

---

## 1. Build and push the image

```bash
# Build multi-platform image and push to ghcr.io
just image

# Or manually:
docker build --platform linux/amd64,linux/arm64 \
  -t ghcr.io/paulgessinger/sentinel:sha-$(git rev-parse --short HEAD) .
docker push ghcr.io/paulgessinger/sentinel:sha-$(git rev-parse --short HEAD)
docker tag ghcr.io/paulgessinger/sentinel:sha-$(git rev-parse --short HEAD) \
           ghcr.io/paulgessinger/sentinel:latest
docker push ghcr.io/paulgessinger/sentinel:latest
```

---

## 2. Create the namespace (if needed)

```bash
oc new-project sentinel
# or
kubectl create namespace sentinel
```

---

## 3. Configure credentials

**Option A — Pass credentials directly on the command line (not recommended for CI):**

```bash
helm upgrade --install sentinel ./helm \
  --namespace sentinel \
  --set github.appId="<APP_ID>" \
  --set github.webhookSecret="<WEBHOOK_SECRET>" \
  --set-file github.privateKey=path/to/private-key.pem
```

**Option B — Create a values file (recommended):**

Create `my-values.yaml` (do not commit this file):

```yaml
github:
  appId: "123456"
  webhookSecret: "my-webhook-secret"
  privateKey: |
    -----BEGIN RSA PRIVATE KEY-----
    MIIEowIBAAKCAQEA...
    -----END RSA PRIVATE KEY-----
```

Then deploy:

```bash
helm upgrade --install sentinel ./helm \
  --namespace sentinel \
  -f my-values.yaml
```

**Option C — Pre-create an OpenShift/Kubernetes Secret:**

```bash
oc create secret generic sentinel-github \
  --from-literal=GITHUB_APP_ID="<APP_ID>" \
  --from-literal=GITHUB_WEBHOOK_SECRET="<WEBHOOK_SECRET>" \
  --from-file=GITHUB_PRIVATE_KEY=path/to/private-key.pem \
  -n sentinel
```

Then reference it in `my-values.yaml`:

```yaml
github:
  existingSecret: sentinel-github
```

---

## 4. Choose a StorageClass

Check what's available in your cluster:

```bash
oc get storageclass
```

Set the StorageClass in your values file:

```yaml
persistence:
  storageClass: "ocs-storagecluster-cephfs"   # example for ODF/OCS
  size: 5Gi
```

---

## 5. First-time install

```bash
helm upgrade --install sentinel ./helm \
  --namespace sentinel \
  -f my-values.yaml \
  --wait
```

`--wait` blocks until all pods are ready and will surface any startup failures.

After install, Helm prints the app URL. You can also retrieve it with:

```bash
oc get route sentinel -n sentinel -o jsonpath='{.spec.host}'
```

---

## 6. Upgrading

### Deploy a new image tag

```bash
helm upgrade sentinel ./helm \
  --namespace sentinel \
  -f my-values.yaml \
  --set image.tag="sha-abc1234"
```

### End-to-end workflow (build → deploy)

```bash
just deploy
```

This builds and pushes the image, then runs `helm upgrade --install` with the
image pinned to the current commit SHA. It blocks until the new pod is healthy
(`--wait`), so a failed migration or crash is surfaced immediately.

The recipe reads credentials from `my-values.yaml` in the repo root (not
committed). Override the file or namespace with Justfile variables:

```bash
just helm_values=staging.yaml helm_namespace=sentinel-staging deploy
```

### Rollback

```bash
# List history
helm history sentinel -n sentinel

# Roll back to a previous revision
helm rollback sentinel <REVISION> -n sentinel
```

---

## 7. Database migrations

Migrations run automatically as an **init container** every time a pod starts:

```
sentinel migrate-webhook-db
```

This is idempotent — running it against an already-current schema is a no-op.

To run migrations manually:

```bash
oc exec -n sentinel deploy/sentinel-web -c migrate -- sentinel migrate-webhook-db
```

---

## 8. Verify the deployment

```bash
# Pod status
oc get pods -n sentinel

# Logs
oc logs -n sentinel deploy/sentinel-web -c web -f

# Health check
curl https://$(oc get route sentinel -n sentinel -o jsonpath='{.spec.host}')/status

# Prometheus metrics
curl https://$(oc get route sentinel -n sentinel -o jsonpath='{.spec.host}')/metrics
```

---

## 9. Configure the GitHub App webhook

1. In your GitHub App settings, set the webhook URL to:
   ```
   https://<route-hostname>/webhook
   ```
2. Set the webhook secret to match `github.webhookSecret`.
3. Subscribe to at least: `Check runs`, `Check suites`, `Pull requests`, `Statuses`.

---

## 10. Uninstall

```bash
helm uninstall sentinel -n sentinel

# The PVC is NOT deleted automatically — delete manually if desired:
oc delete pvc sentinel-data -n sentinel
```

---

## Configuration reference

All values can be overridden in your `my-values.yaml`.
Below are the most commonly changed settings.

| Value | Default | Description |
|---|---|---|
| `image.tag` | `latest` | Image tag to deploy |
| `web.replicas` | `1` | Web server replicas |
| `persistence.storageClass` | `""` | StorageClass for the PVC |
| `persistence.size` | `5Gi` | PVC size |
| `persistence.accessMode` | `ReadWriteMany` | PVC access mode |
| `route.host` | `""` | Custom hostname (auto-assigned if empty) |
| `route.tls.termination` | `edge` | TLS termination (`edge`, `passthrough`, `reencrypt`) |
| `config.logLevel` | `WARNING` | Log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `config.projectionEvalEnabled` | `false` | Enable projection-driven evaluation |
| `config.projectionPublishEnabled` | `false` | Enable posting check runs from projections |
| `config.repoAllowlist` | `""` | Comma-separated list of private repos to process |
| `telegram.enabled` | `false` | Enable Telegram notifications |

See [helm/values.yaml](helm/values.yaml) for the full list.

---

## Security notes

- The Dockerfile runs the process as a non-root `sentinel` user (UID 1000),
  which is compatible with OpenShift's `restricted` SCC.
- GitHub credentials (App ID, webhook secret, private key) are stored in an
  OpenShift Secret and are never written to the ConfigMap.
- The private key is injected as an environment variable (`GITHUB_PRIVATE_KEY`).
  Consider using a Secret volume mount if your security policy requires it.
