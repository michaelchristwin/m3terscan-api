FROM python:3.14.3-bookworm

# Copy uv binary from official image
COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1

# Copy only dependency files first (better caching)
COPY pyproject.toml uv.lock ./

RUN uv sync --frozen && uv cache prune --ci

# Then copy rest of code
COPY . .

EXPOSE 3000

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-w", "4", "main:app", "--bind", "0.0.0.0:3000"]
