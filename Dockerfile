# Verified release baseline. Review and update this digest for security fixes.
ARG PYTHON_IMAGE=python:3.12.14-bookworm@sha256:dbbe4ceb97851e2e5fa83798b239811f871cb743b259ba3563737349f6bcfaa0
FROM ${PYTHON_IMAGE} AS base

WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
COPY src ./src

FROM base AS tested
COPY tests ./tests
RUN python -m unittest discover -v

FROM base AS runtime
COPY --from=tested /app/src ./src
ENV CRYPTO_SYNC_LOG_LEVEL=INFO
CMD ["python", "-m", "src.main"]
