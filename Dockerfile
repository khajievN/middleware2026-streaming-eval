# Enclave bench image. Built on amazonlinux:2 to match the proven production
# enclave runtime (epsilon-enclave): minimal Debian images (python:slim) fail to
# boot as a Nitro enclave on this host. Stdlib only — no third-party deps, so no
# pip and no import-time failure modes inside the restricted enclave.
FROM amazonlinux:2

RUN amazon-linux-extras install python3.8 -y && \
    yum install -y python38 && \
    yum clean all && \
    alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1

WORKDIR /app
COPY framing.py enclave_bench.py /app/

ENV PYTHONUNBUFFERED=1
CMD ["python3", "/app/enclave_bench.py", "--transport", "vsock", "--port", "5006"]
