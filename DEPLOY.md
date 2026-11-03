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

### 4. Configure the GitHub App

**Repository permissions:**

| Permission | Level | Needed for |
| --- | --- | --- |
| Actions | Read | workflow run state |
| Checks | Read & write | reading CI checks and publishing `merge-sentinel` |
| Contents | Read | `.merge-sentinel.yml` and the compare endpoint used by path rules |
| Merge queues | Read | receiving `merge_group` webhook events |
| Metadata | Read | mandatory baseline |
| Pull requests | Read | PR head/base state and changed files |
| Statuses | Read | commit status contexts |

**Webhook:**

1. Set the webhook URL to `https://<route-hostname>/webhook`
2. Confirm the webhook secret matches what you passed above
3. Subscribe to: `Check runs`, `Check suites`, `Pull requests`, `Statuses`, `Merge group`

Permission changes must be accepted by whoever administers each installation of
the App, so batch them into a single approval rather than requesting them one at
a time.

---

## Merge queue support

Sentinel gates GitHub merge queue entries by publishing `merge-sentinel` onto the
queue entry's synthetic merge commit. Three prerequisites, none of which activate
on their own:

1. **`Merge queues: Read` permission.** Without it the `Merge group` webhook event
   cannot be delivered at all, regardless of the subscription setting or anything
   in the code.
2. **`Contents: Read` permission.** Path rules (`paths` / `paths_ignore`) resolve a
   queue entry's changed files via
   `GET /repos/{owner}/{repo}/compare/{base_sha}...{head_sha}`. A narrower
   `single_file: read` scope covers only `.merge-sentinel.yml` and does **not**
   cover the compare endpoint — the call returns `403` for every repo whose config
   uses path rules.
3. **`Merge group` webhook subscription**, plus `merge_group` present in
   `WEBHOOK_DB_EVENTS` (already set in [deploy/configmap.yaml](deploy/configmap.yaml)).

Both permissions are App-level and therefore apply to every repository the App
serves, not only the ones using a merge queue. Repositories without a merge queue
emit no `merge_group` events and are unaffected.

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

## Reconciliation (missed webhooks)

The running web server periodically sweeps for open PRs whose sentinel check
looks stuck — missing, non-terminal, or not re-evaluated in
`PROJECTION_RECONCILE_STALE_SECONDS` — and forces the same API-refresh path the
"Re-evaluate now" check-run action uses. This recovers PRs stuck behind a webhook
GitHub never delivered, or one dropped before it reached the projection scheduler.
Controlled by `PROJECTION_RECONCILE_ENABLED` / `_INTERVAL_SECONDS` /
`_STALE_SECONDS` in [deploy/configmap.yaml](deploy/configmap.yaml).

The sweep runs every minute. PRs become eligible after five quiet minutes and
merge queue entries after two (`PROJECTION_RECONCILE_MERGE_GROUP_STALE_SECONDS`).
API budget restrictions can delay recovery; these intervals are not a deadline.

All GitHub HTTP requests, including pagination and check publishing, pass through
a shared guard. Across the app, requests are serialized and spaced by at least
one second, and writes by eight seconds. Per-installation response headers adjust pacing and retain
100 requests of primary-limit headroom. Primary-limit responses pause that installation
using `Retry-After` and the primary reset time. Secondary-limit responses pause the
whole app with exponential backoff.
Deferred evaluations can be retried by the next sweep or webhook; no retry loop
continues sending requests during the cooldown.

Budget and lock files live under `${DISKCACHE_DIR}/github-rate`. Server workers and
CLI commands must share that directory on storage supporting advisory file locks.
Separate volumes or other applications using the same installation credentials
cannot be coordinated by this guard. GitHub's undisclosed secondary limits mean
no client can promise never to receive a rate-limit response.

To force a refresh for a specific repo right away instead of waiting for the next
sweep:

```bash
just reconcile acts-project/acts --dry-run   # preview
just reconcile acts-project/acts             # actually refresh
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
