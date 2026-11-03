#!/usr/bin/env bash
set -euo pipefail

# Build only a committed source snapshot. No local credentials or caches enter
# the binary build context. --deploy applies runtime manifests and pins images.
namespace=${OC_NAMESPACE:-merge-sentinel}
deploy=false
if [[ ${1:-} == --deploy ]]; then
    deploy=true
    shift
fi
if [[ $# != 0 ]]; then
    echo "Usage: $0 [--deploy]" >&2
    exit 2
fi
cd "$(git rev-parse --show-toplevel)"
if ! git diff --quiet || ! git diff --cached --quiet ||
    [[ -n $(git ls-files --others --exclude-standard -- Dockerfile pyproject.toml uv.lock src deploy scripts) ]]; then
    echo "Commit or set aside local changes before building a revision-tagged image." >&2
    exit 1
fi
revision=$(git rev-parse HEAD)
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT
git archive --format=tar HEAD Dockerfile pyproject.toml uv.lock src > "$temporary/source.tar"
oc apply -n "$namespace" -f deploy/imagestream.yaml -f deploy/buildconfig.yaml >&2
build=$(oc start-build merge-sentinel -n "$namespace" \
    --from-archive="$temporary/source.tar" --wait -o name)
phase=$(oc get -n "$namespace" "$build" -o jsonpath='{.status.phase}')
if [[ $phase != Complete ]]; then
    echo "Build $build ended in phase $phase" >&2
    exit 1
fi
digest=$(oc get -n "$namespace" "$build" -o jsonpath='{.status.output.to.imageDigest}')
reference=$(oc get -n "$namespace" "$build" -o jsonpath='{.status.outputDockerImageReference}')
if [[ ! $digest =~ ^sha256:[a-f0-9]{64}$ || -z $reference ]]; then
    echo "Build did not return an image reference and digest; refusing to deploy." >&2
    exit 1
fi
image="${reference%:*}@$digest"
oc tag -n "$namespace" "$image" "merge-sentinel:sha-$revision" >&2
if $deploy; then
    oc apply -n "$namespace" -f deploy/configmap.yaml -f deploy/pvc.yaml \
        -f deploy/service.yaml -f deploy/route.yaml >&2
    oc set image --local -f deploy/deployment.yaml web="$image" migrate="$image" -o yaml |
        oc apply -n "$namespace" -f - >&2
    oc rollout status -n "$namespace" deployment/merge-sentinel-web --timeout=300s >&2
fi
printf '%s\n' "$image"
