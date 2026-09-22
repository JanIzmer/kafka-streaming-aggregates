# ---------------------------------------------------------------------------
# One image, two entrypoints. Producer and processor share the models, the
# contract and the codec, so building them separately would let the two halves
# drift apart in exactly the place that matters.
# ---------------------------------------------------------------------------
FROM python:3.14-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install --no-install-recommends -y gcc libpq5 curl \
    && rm -rf /var/lib/apt/lists/*

# Dependencies first so the layer is cached until requirements actually change.
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY pyproject.toml /app/pyproject.toml
COPY src /app/src
COPY contracts /app/contracts
RUN pip install --no-deps -e /app

# Non-root: the processor needs no filesystem access beyond reading its own code.
RUN useradd --create-home --uid 10001 stream && chown -R stream:stream /app
USER stream

ENV PYTHONPATH=/app/src
CMD ["orders-processor", "run"]
