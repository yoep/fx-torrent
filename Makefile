.PHONY: cargo

prerequisites: ## Install the Cargo requirements for testing
	@echo Installing Cargo plugins
	@command -v cargo-nextest >/dev/null 2>&1 || cargo install cargo-nextest --locked
	@command -v cargo-llvm-cov >/dev/null 2>&1 || cargo install cargo-llvm-cov

test: prerequisites ## Test the cargo project
	@cargo nextest run

test-coverage: prerequisites ## Test the cargo project with coverage report
	@cargo llvm-cov --lcov --output-path target/lcov.info nextest

cov: ## Test the cargo project with coverage reporting to stdout
	@cargo llvm-cov nextest

build: ## Build the cargo project
	@cargo build

build-release: ## Build the release version of the cargo project
	@cargo build --release

fmt: ## Run the cargo formatter
	@cargo fmt --all -- --check

bump-%: ## Bump the (major, minor, patch) version of the application
	@bump-my-version bump $*
