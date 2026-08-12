fmt:
	cargo fmt --all
	cargo clippy --all --fix --allow-staged --allow-dirty -- -D warnings

check-fmt:
	cargo fmt --all -- --check
	cargo clippy --all --all-features -- -D unsafe_op_in_unsafe_fn

test:
	cargo test --all --all-features -- --test-threads=1

BENCH_RUNNER = cargo run --release --quiet -p cyclonedds-bench --bin runner --
BENCH_OUT ?= target/bench-results
BENCH_RUN_ID ?= local-$(shell date -u +%Y%m%dT%H%M%SZ)
BENCH_RUN_ID := $(BENCH_RUN_ID)
BENCH_DIR := $(BENCH_OUT)/$(BENCH_RUN_ID)
BENCH_METADATA := $(BENCH_DIR)/metadata.json
BENCH_RESULTS := $(BENCH_DIR)/results.jsonl

.PHONY: check-actions update-action-pins build-local-bench test-local-smoke test-local-cdr test-local-udp test-local-shm test-local-all

check-actions:
	aqua exec -- actionlint
	aqua exec -- pinact run --check

update-action-pins:
	aqua exec -- pinact run --update

build-local-bench:
	cargo build --release -p cyclonedds-bench

test-local-smoke: build-local-bench
	mkdir -p $(BENCH_DIR)
	cp bench/cases.json $(BENCH_DIR)/cases.json
	$(BENCH_RUNNER) --case smoke-256b-udp --run-id $(BENCH_RUN_ID) --output $(BENCH_RESULTS) --metadata $(BENCH_METADATA)

test-local-cdr: build-local-bench
	mkdir -p $(BENCH_DIR)
	cp bench/cases.json $(BENCH_DIR)/cases.json
	$(BENCH_RUNNER) --case cdr-256b --run-id $(BENCH_RUN_ID) --output $(BENCH_RESULTS) --metadata $(BENCH_METADATA)
	$(BENCH_RUNNER) --case cdr-16kib --run-id $(BENCH_RUN_ID) --output $(BENCH_RESULTS) --metadata $(BENCH_METADATA)
	$(BENCH_RUNNER) --case cdr-1mib --run-id $(BENCH_RUN_ID) --output $(BENCH_RESULTS) --metadata $(BENCH_METADATA)

test-local-udp: build-local-bench
	mkdir -p $(BENCH_DIR)
	cp bench/cases.json $(BENCH_DIR)/cases.json
	$(BENCH_RUNNER) --case latency-256b-udp --run-id $(BENCH_RUN_ID) --output $(BENCH_RESULTS) --metadata $(BENCH_METADATA)
	$(BENCH_RUNNER) --case throughput-boundary-below-udp --run-id $(BENCH_RUN_ID) --output $(BENCH_RESULTS) --metadata $(BENCH_METADATA)
	$(BENCH_RUNNER) --case throughput-boundary-above-udp --run-id $(BENCH_RUN_ID) --output $(BENCH_RESULTS) --metadata $(BENCH_METADATA)

test-local-shm: build-local-bench
	mkdir -p $(BENCH_DIR)
	cp bench/cases.json $(BENCH_DIR)/cases.json
	$(BENCH_RUNNER) --case throughput-1mib-shm --run-id $(BENCH_RUN_ID) --output $(BENCH_RESULTS) --metadata $(BENCH_METADATA)

test-local-all: test-local-smoke test-local-cdr test-local-udp test-local-shm
