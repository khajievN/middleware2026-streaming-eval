# Middleware 2026 streaming evaluation — c5a.xlarge runbook.
#
# Order on the EC2 parent (Nitro-enabled, allocator configured for 4096 MB / 2 vCPU):
#   make scale            # one-time: build the 10 GB synthetic CSV
#   make image eif        # build the bench enclave image + EIF
#   make run-enclave      # boot the enclave (debug mode -> console logs)
#   make throughput       # Graph 2 data
#   make oom              # Graph 1 data
#   make plot             # render both figures from results/*.json

# --- paths ---
DEMO_LABEVENTS ?= $(HOME)/Developments/WebStorm/phd_milestone/datasets/mimic-iv-clinical-database-demo-2.2/hosp/labevents.csv.gz
SCALED_CSV     ?= data/labevents_scaled.csv
TARGET_GB      ?= 10
RESULTS        ?= results

# --- enclave params (c5a.xlarge: 4 vCPU total -> 2 to parent, 2 to enclave) ---
IMAGE      ?= mw-stream-bench
EIF        ?= bench.eif
ENCLAVE_MEM ?= 4096
ENCLAVE_CPUS ?= 2
ENCLAVE_CID ?= 16
INSTANCE   ?= c5a.xlarge

# --- transport (override TRANSPORT=tcp for a laptop smoke test) ---
TRANSPORT  ?= vsock
PORT       ?= 5006

scale:
	python3 scale_mimic.py --source $(DEMO_LABEVENTS) --out $(SCALED_CSV) --target-gb $(TARGET_GB)

image:
	docker build -t $(IMAGE):latest .

eif: image
	nitro-cli build-enclave --docker-uri $(IMAGE):latest --output-file $(EIF)

run-enclave:
	nitro-cli terminate-enclave --all || true
	nitro-cli run-enclave --eif-path $(EIF) --memory $(ENCLAVE_MEM) \
		--cpu-count $(ENCLAVE_CPUS) --enclave-cid $(ENCLAVE_CID) --debug-mode
	@echo "console logs: nitro-cli console --enclave-id \$$(nitro-cli describe-enclaves | jq -r '.[0].EnclaveID')"

throughput:
	python3 parent_driver.py --transport $(TRANSPORT) --cid $(ENCLAVE_CID) --port $(PORT) \
		--instance-type $(INSTANCE) --enclave-memory-mb $(ENCLAVE_MEM) --enclave-cpus $(ENCLAVE_CPUS) \
		--out $(RESULTS)/throughput.json throughput

oom:
	python3 parent_driver.py --transport $(TRANSPORT) --cid $(ENCLAVE_CID) --port $(PORT) \
		--instance-type $(INSTANCE) --enclave-memory-mb $(ENCLAVE_MEM) --enclave-cpus $(ENCLAVE_CPUS) \
		--out $(RESULTS)/oom.json oom --data $(SCALED_CSV) --stop-on-oom

plot:
	python3 plot.py --results-dir $(RESULTS)

# Laptop smoke test over TCP (no Nitro): runs server in the background, drives a
# tiny sweep, then stops the server. Validates the protocol + workloads end to end.
smoke:
	python3 enclave_bench.py --transport tcp --port $(PORT) & echo $$! > .smoke.pid; sleep 2; \
	python3 parent_driver.py --transport tcp --cid 3 --port $(PORT) --out $(RESULTS)/smoke_throughput.json \
		throughput --total-bytes 33554432 --trials 2 --chunk-sizes 65536,1048576 ; \
	python3 parent_driver.py --transport tcp --cid 3 --port $(PORT) --enclave-memory-mb 4096 \
		--out $(RESULTS)/smoke_oom.json oom --data $(SCALED_CSV) --sizes-mb 20,50 --chunk-size 1048576 ; \
	kill `cat .smoke.pid` && rm -f .smoke.pid

clean:
	nitro-cli terminate-enclave --all || true
	rm -f $(EIF) .smoke.pid

.PHONY: scale image eif run-enclave throughput oom plot smoke clean
