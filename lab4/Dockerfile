FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# Dependencies first, so this layer is cached until pyproject.toml / uv.lock change
RUN pip install --no-cache-dir uv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app.py cache.py ./
COPY queries/ queries/

USER 65532:65532
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
