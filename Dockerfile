FROM python:3.13-slim-bookworm

# Copy uv binary from official image
COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/

WORKDIR /app
# Prevent uv from creating virtualenv (recommended in containers)
ENV UV_SYSTEM_PYTHON=1


# Copy only dependency files first (better caching)
COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev && rm -rf ~/.cache/uv

# Then copy rest of code
COPY . .

EXPOSE 3000

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-w", "4", "main:app", "--bind", "0.0.0.0:3000"]
