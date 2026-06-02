# Minimal enclave image for the streaming/OOM benchmark.
# Deliberately excludes the KMS/NSM attestation stack: this experiment measures
# the VSock transport and the enclave memory model, not the crypto path, so a
# lean image keeps the measurement free of attestation-side confounds.
FROM python:3.11-slim

# No third-party deps: the workloads use only the standard library, which keeps
# the enclave image minimal and removes import-time failure modes in the
# restricted enclave environment.
WORKDIR /app
COPY framing.py enclave_bench.py /app/

ENV PYTHONUNBUFFERED=1
CMD ["python3", "/app/enclave_bench.py", "--transport", "vsock", "--port", "5006"]
