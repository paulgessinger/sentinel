# Start the dev server with hot-reload using .env.dev
run:
   dotenvx run -f .env.dev -- uv run sanic sentinel.web:create_app --factory --port 8080 --host 0.0.0.0 --reload --dev

# Run the test suite
test:
    uv run pytest

# Remove local disk cache files
clean:
    @rm -rf cache/files cache/*

image_url := "ghcr.io/paulgessinger/sentinel"
sha := "sha-" + `git rev-parse --short HEAD`

# Build multi-arch Docker image (amd64 + arm64) tagged with current git SHA
image_build:
    docker build --platform linux/amd64,linux/arm64 -t {{image_url}}:{{sha}} .

# Build image, tag as latest, and push both SHA and latest tags to the registry
image: image_build
    docker tag {{image_url}}:{{sha}} {{image_url}}:latest
    docker push {{image_url}}:{{sha}}
    docker push {{image_url}}:latest


oc_namespace := "merge-sentinel"

# Apply all manifests (idempotent; run after changing files in deploy/)
apply:
    oc apply -f deploy/ -n {{oc_namespace}}

# Apply only the configmap manifest (pods need a restart to pick up new values)
apply_config:
    oc apply -f deploy/configmap.yaml -n {{oc_namespace}}

# Watch rollout status of the web deployment
rollout:
    oc rollout status deployment/merge-sentinel-web -n {{oc_namespace}}

# Restart the web deployment pods in the local cluster
restart:
    oc rollout restart deployment/merge-sentinel-web -n {{oc_namespace}}

# Project all open PRs of a repo into pr_heads_current on the prod DB.
# Run after rebuilding the database, otherwise PRs with no further webhook activity
# stay invisible to evaluation. Pass --dry-run to preview.
# Usage: just backfill_prs acts-project/acts <installation-id> [--dry-run]
backfill_prs repo installation *flags:
    oc exec -n {{oc_namespace}} deployment/merge-sentinel-web -c web -- \
        sentinel backfill-pull-requests {{repo}} {{installation}} {{flags}}

# Force-refresh open PRs whose sentinel check looks stuck (missed webhook, etc.).
# The running server already sweeps for this periodically (PROJECTION_RECONCILE_*
# in deploy/configmap.yaml); use this to kick a repo on demand. Pass --dry-run to preview.
# Usage: just reconcile acts-project/acts [--dry-run]
reconcile repo *flags:
    oc exec -n {{oc_namespace}} deployment/merge-sentinel-web -c web -- \
        sentinel reconcile {{repo}} {{flags}}

# Update the web and migrate container images to the current git SHA in the local cluster
update_image:
    oc set image deployment/merge-sentinel-web web={{image_url}}:{{sha}} migrate={{image_url}}:{{sha}} -n {{oc_namespace}}

# Build image, apply manifests, roll out new version
deploy: image update_image
    oc rollout status deployment/merge-sentinel-web -n {{oc_namespace}}

# Run the image locally with dev env vars, mounting the cache volume
docker: image_build
    docker run --rm -it --env-file .env.dev -e DISKCACHE_DIR=/cache -v$PWD/cache:/cache -p8080:8080 {{image_url}}:{{sha}}

# Forward GitHub webhook events from smee.io to the local dev server
smee:
    smee -u https://smee.io/k8smrd9B27JqKRBg -t http://localhost:8080/webhook

# Forward GitHub webhook events from smee.io to the production endpoint
smee-remote:
    smee -u https://smee.io/k8smrd9B27JqKRBg -t https://merge-sentinel.app.cern.ch/webhook


# Run ruff linter (with auto-fix), ruff formatter, and ty type checker
lint:
    uv run ruff check --fix .
    uv run ruff format .
    uv run ty check .
