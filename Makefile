fmt:
	cargo fmt --all
	cargo clippy --all --fix --allow-staged --allow-dirty -- -D warnings

check-fmt:
	cargo fmt --all -- --check
	cargo clippy --all --all-features -- -D unsafe_op_in_unsafe_fn

test:
	cargo test --all --all-features -- --test-threads=1
