run:
    dotenvx run -f .env.dev -- uv run sanic sentinel.web:create_app --factory --port 8080 --host 0.0.0.0 --reload --dev

test:
    uv run pytest

clean:
    @rm -rf cache/files cache/*

image_url := "ghcr.io/paulgessinger/sentinel"
sha := "sha-" + `git rev-parse --short HEAD`

image_build:
    docker build --platform linux/amd64,linux/arm64 -t {{image_url}}:{{sha}} .

image: image_build
    docker tag {{image_url}}:{{sha}} {{image_url}}:latest
    docker push {{image_url}}:{{sha}}
    docker push {{image_url}}:latest


oc_namespace := "merge-sentinel"

# Apply all manifests (idempotent; run after changing files in deploy/)
apply:
    oc apply -f deploy/ -n {{oc_namespace}}

apply_config_remote:
    cat deploy/configmap.yaml | ssh lxplus oc apply -f - -n {{oc_namespace}}

rollout:
    oc rollout status deployment/merge-sentinel-web -n {{oc_namespace}}

restart:
    oc rollout restart deployment/merge-sentinel-web -n {{oc_namespace}}

restart_remote:
    ssh lxplus oc rollout restart deployment/merge-sentinel-web -n {{oc_namespace}}

update_image:
    oc set image deployment/merge-sentinel-web web={{image_url}}:{{sha}} migrate={{image_url}}:{{sha}} -n {{oc_namespace}}

update_image_remote:
    ssh lxplus oc set image deployment/merge-sentinel-web web={{image_url}}:{{sha}} migrate={{image_url}}:{{sha}} -n {{oc_namespace}}

# Build image, apply manifests, roll out new version
deploy: image update_image
    oc rollout status deployment/merge-sentinel-web -n {{oc_namespace}}

deploy_remote: image update_image_remote
    ssh lxplus oc rollout status deployment/merge-sentinel-web -n {{oc_namespace}}

docker: image_build
    docker run --rm -it --env-file .env.dev -e DISKCACHE_DIR=/cache -v$PWD/cache:/cache -p8080:8080 {{image_url}}:{{sha}}

smee:
    smee -u https://smee.io/k8smrd9B27JqKRBg -t http://localhost:8080/webhook

smee-remote:
    smee -u https://smee.io/k8smrd9B27JqKRBg -t https://merge-sentinel.app.cern.ch/webhook


lint:
    uv run ruff check --fix .
    uv run ruff format .
    uv run ty check .
