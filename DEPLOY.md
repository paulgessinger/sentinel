# Deploying Sentinel to OpenShift

## Prerequisites

- OpenShift cluster with `oc` CLI configured and logged in
- A GitHub App with a private key, app ID, and webhook secret
- Docker with buildx for multi-platform builds

---

## One-time setup

### 1. Create the project

```bash
oc new-project merge-sentinel
```

### 2. Create the GitHub credentials secret

```bash
oc create secret generic merge-sentinel-github \
  --from-literal=GITHUB_APP_ID="<APP_ID>" \
  --from-literal=GITHUB_WEBHOOK_SECRET="<WEBHOOK_SECRET>" \
  --from-file=GITHUB_PRIVATE_KEY=path/to/private-key.pem \
  -n merge-sentinel
```

This secret is never committed to the repo. Re-run the command if credentials rotate.

### 3. Apply all manifests and do the first deploy

```bash
just deploy
```

This builds and pushes the image, applies all files in `deploy/`, and rolls out
the deployment. OpenShift will auto-assign a hostname for the Route; retrieve it with:

```bash
oc get route merge-sentinel -n merge-sentinel -o jsonpath='{.spec.host}'
```

### 4. Configure the GitHub App webhook

In your GitHub App settings:
1. Set the webhook URL to `https://<route-hostname>/webhook`
2. Confirm the webhook secret matches what you passed above
3. Subscribe to: `Check runs`, `Check suites`, `Pull requests`, `Statuses`

---

## Day-to-day deploy

```bash
just deploy
```

Builds and pushes a new image tagged with the current commit SHA, applies any
manifest changes, then does a rolling update. Blocks until the new pod is ready.

---

## Manifest-only changes

To apply changes to files in `deploy/` without building a new image:

```bash
just apply
```

---

## Rollback

```bash
oc rollout undo deployment/merge-sentinel-web -n merge-sentinel
```

To roll back to a specific revision:

```bash
oc rollout history deployment/merge-sentinel-web -n merge-sentinel
oc rollout undo deployment/merge-sentinel-web --to-revision=<N> -n merge-sentinel
```

---

## Database migrations

Migrations run automatically before the web server starts on every pod restart
(via an init container that runs `sentinel migrate-webhook-db`). This is
idempotent — no action needed on normal deploys.

To run migrations manually:

```bash
oc exec -n merge-sentinel deployment/merge-sentinel-web -- sentinel migrate-webhook-db
```

---

## Useful commands

```bash
# Pod status
oc get pods -n merge-sentinel

# Logs (follow)
oc logs -n merge-sentinel deployment/merge-sentinel-web -c web -f

# Health check
curl https://$(oc get route merge-sentinel -n merge-sentinel -o jsonpath='{.spec.host}')/status

# Prometheus metrics
curl https://$(oc get route merge-sentinel -n merge-sentinel -o jsonpath='{.spec.host}')/metrics
```

---

## Uninstall

```bash
oc delete -f deploy/ -n merge-sentinel
oc delete secret merge-sentinel-github -n merge-sentinel

# PVC is not deleted by the above — remove manually if desired:
oc delete pvc merge-sentinel-data -n merge-sentinel
```

---

## Configuration

Non-sensitive settings live in [deploy/configmap.yaml](deploy/configmap.yaml).
Edit the file and run `just apply` to pick up changes (a pod restart is needed
for the new values to take effect).

Sensitive values (GitHub credentials) are in the `merge-sentinel-github` Secret
created during setup. See the one-time setup section to recreate it.
