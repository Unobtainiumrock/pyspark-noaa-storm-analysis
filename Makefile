# Distributed Computing Development Environment
# Single entry point for all development operations

.PHONY: help build run run-bg stop clean logs url shell format lint kill-spark cleanup env-setup setup dev

# Environment variable handling
ENV_ARGS := $(shell if [ -f .env ]; then echo "--env-file .env"; fi)

# Default target
help: ## Show this help message
	@echo "Distributed Computing Development Environment"
	@echo "=============================================="
	@echo ""
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Environment Variables:"
	@echo "  Create .env file from env.example template for secure variable handling"

# Environment setup
env-setup: ## Create .env file from template (if it doesn't exist)
	@if [ ! -f .env ]; then \
		echo "Creating .env file from template..."; \
		cp env.example .env; \
		echo " .env file created! Edit it with your values."; \
		echo "  Remember: .env files are ignored by git for security."; \
	else \
		echo ".env file already exists."; \
	fi

# Build the Docker image
build: ## Build the development environment Docker image
	@echo "Building PySpark development environment..."
	docker build -t pyspark-dev-env .
	@echo "Build complete! Image tagged as 'pyspark-dev-env'"

# Run the development environment
run: ## Start the development environment (builds if needed)
	@echo "Starting PySpark development environment..."
	@if ! docker image inspect pyspark-dev-env >/dev/null 2>&1; then \
		echo "Image not found. Building first..."; \
		$(MAKE) build; \
	fi
	@if docker ps --format '{{.Names}}' | grep -q '^pyspark-dev$$'; then \
		echo " Container 'pyspark-dev' is already running"; \
		echo " Use 'make shell' to access it, or 'make stop' to stop it first"; \
	else \
		if docker ps -a --format '{{.Names}}' | grep -q '^pyspark-dev$$'; then \
			echo " Container exists but is stopped. Removing it for interactive session..."; \
			docker rm pyspark-dev 2>/dev/null || true; \
		fi; \
		docker run -it --rm \
			--name pyspark-dev \
			-p 8888:8888 \
			-v "$(PWD)":/home/sparkdev/app \
			$(ENV_ARGS) \
			pyspark-dev-env; \
	fi

# Run in background (detached mode)
run-bg: ## Start the development environment in background
	@echo "Starting PySpark development environment in background..."
	@if ! docker image inspect pyspark-dev-env >/dev/null 2>&1; then \
		echo "Image not found. Building first..."; \
		$(MAKE) build; \
	fi
	@if docker ps -a --format '{{.Names}}' | grep -q '^pyspark-dev$$'; then \
		if docker ps --format '{{.Names}}' | grep -q '^pyspark-dev$$'; then \
			echo " Container 'pyspark-dev' is already running"; \
			echo " JupyterLab should be available at: http://localhost:8888"; \
			echo " Use 'make url' to get the access token"; \
		else \
			echo " Container 'pyspark-dev' exists but is stopped. Starting it..."; \
			docker start pyspark-dev; \
			echo " Container started"; \
			echo " JupyterLab will be available at: http://localhost:8888"; \
			echo " Use 'make logs' to see the startup logs and access token"; \
		fi \
	else \
		echo " Creating new container..."; \
		docker run -d \
			--name pyspark-dev \
			-p 8888:8888 \
			-v "$(PWD)":/home/sparkdev/app \
			$(ENV_ARGS) \
			pyspark-dev-env \
			/opt/venv/bin/jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root; \
		echo " Container started in background"; \
		echo " JupyterLab will be available at: http://localhost:8888"; \
		echo " Use 'make logs' to see the startup logs and access token"; \
	fi

# Stop the running container
stop: ## Stop the development environment
	@echo "Stopping development environment..."
	@docker stop pyspark-dev 2>/dev/null || echo "No running container found"
	@docker rm pyspark-dev 2>/dev/null || echo "No container to remove"
	@echo " Development environment stopped"

# Kill any Spark sessions and clean up ports
kill-spark: ## Kill all Spark sessions inside the container
	@echo "Killing Spark sessions..."
	@docker exec pyspark-dev pkill -9 -f "org.apache.spark" 2>/dev/null || echo "No Spark processes found"
	@echo " Spark sessions killed. Ports 4040/4041 should be free now."

# Full cleanup: stop container and remove any lingering processes
cleanup: stop ## Complete cleanup: stop container and kill any Spark processes
	@echo " Complete cleanup finished. Ready for fresh start."

# Show container logs
logs: ## Show logs from the running container
	@if docker ps --format '{{.Names}}' | grep -q '^pyspark-dev$$'; then \
		docker logs pyspark-dev; \
	else \
		echo "No running container found. Use 'make run-bg' first."; \
	fi

# Get JupyterLab URL with access token
url: ## Get the JupyterLab URL with access token for copy-paste
	@echo "JupyterLab URL (copy-paste this into your browser):"
	@URL=$$(docker logs pyspark-dev 2>&1 | grep "127.0.0.1:8888" | head -1 | awk '{print $$NF}' | sed 's/127.0.0.1/localhost/'); \
	if [ -z "$$URL" ]; then \
		echo "Waiting for JupyterLab to start..."; \
		for i in 1 2 3 4 5; do \
			sleep 1; \
			URL=$$(docker logs pyspark-dev 2>&1 | grep "127.0.0.1:8888" | head -1 | awk '{print $$NF}' | sed 's/127.0.0.1/localhost/'); \
			if [ -n "$$URL" ]; then \
				break; \
			fi; \
		done; \
	fi; \
	if [ -n "$$URL" ]; then \
		echo "$$URL"; \
	else \
		echo "http://localhost:8888 (JupyterLab not ready yet - is container running?)"; \
	fi

# Open a shell in the running container
shell: ## Open a bash shell in the running container
	@docker exec -it pyspark-dev bash 2>/dev/null || echo "No running container found. Use 'make run-bg' first."

# Clean up Docker resources
clean: ## Remove Docker image and containers
	@echo "Cleaning up Docker resources..."
	@docker stop pyspark-dev 2>/dev/null || true
	@docker rm pyspark-dev 2>/dev/null || true
	@docker rmi pyspark-dev-env 2>/dev/null || true
	@echo "Cleaning up ports 4040 and 4041..."
	@-pkill -f "SparkUI" 2>/dev/null || true
	@-pkill -f "spark" 2>/dev/null || true
	@-pkill -f "jupyter" 2>/dev/null || true
	@-lsof -ti:4040 | xargs -r kill -9 2>/dev/null || true
	@-lsof -ti:4041 | xargs -r kill -9 2>/dev/null || true
	@echo " Cleanup complete"

# Code quality commands
format: ## Format Python code using black
	@echo "Formatting Python code..."
	@docker exec pyspark-dev black /home/sparkdev/app 2>/dev/null || echo "No running container found. Use 'make run-bg' first."

lint: ## Lint Python code using ruff
	@echo "Linting Python code..."
	@docker exec pyspark-dev ruff check /home/sparkdev/app 2>/dev/null || echo "No running container found. Use 'make run-bg' first."

# Development workflow shortcuts
dev: run-bg ## Quick start: build, run in background, and show URL with token
	@echo " Development environment ready!"
	@echo " Waiting for JupyterLab to start..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if docker logs pyspark-dev 2>&1 | grep -q "127.0.0.1:8888"; then \
			break; \
		fi; \
		sleep 1; \
	done
	@echo ""
	@echo " JupyterLab URL (copy-paste this into your browser):"
	@$(MAKE) url

# Full setup from scratch
setup: clean build run-bg ## Complete setup: clean, build, and start
	@echo " Complete development environment setup finished!"
	@echo " Waiting for JupyterLab to start..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do \
		if docker logs pyspark-dev 2>&1 | grep -q "127.0.0.1:8888"; then \
			break; \
		fi; \
		sleep 1; \
	done
	@echo ""
	@echo " JupyterLab URL (copy-paste this into your browser):"
	@docker logs pyspark-dev 2>&1 | grep "127.0.0.1:8888" | head -1 | awk '{print $$NF}' | sed 's/127.0.0.1/localhost/' || echo "http://localhost:8888 (JupyterLab not ready yet - try: make url)"

