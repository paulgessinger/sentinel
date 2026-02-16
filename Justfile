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

image:
    docker tag {{image_url}}:{{sha}} {{image_url}}:latest
    docker push {{image_url}}:{{sha}}
    docker push {{image_url}}:latest


deploy: image
    sleep 1
    oc import-image sentinel --all

docker: image_build
    docker run --rm -it --env-file .env {{image_url}}:{{sha}}

smee:
    smee -u https://smee.io/k8smrd9B27JqKRBg -t http://localhost:8080/webhook
