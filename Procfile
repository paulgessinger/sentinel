#web: uvicorn sentinel.web:create_app --factory --port $PORT --host 0.0.0.0
web: sanic sentinel.web:create_app --factory --port $PORT --host 0.0.0.0
worker: sentinel worker