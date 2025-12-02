The analysis explores the human and economic impact of severe weather, focusing on several key research questions:

* **Harm & Impact Analysis**: Identifying which states, counties, and event types cause the most human harm (injuries/deaths) and assessing the economic damage from floods.

* **Trend & Correlation**: Analyzing historical trends in storm magnitude, mapping tornado frequency by state, and exploring the correlation between storm types and location.

* **Predictive Modeling**: Using a Random Forest model to determine the strongest predictors of human harm, including feature-engineering population density to weigh human vs. meteorological factors.

## Development Environment

This project includes a Docker-based development environment with PySpark, JupyterLab, and all necessary dependencies.

### Quick Start

```bash
# Start the development environment (builds if needed)
make dev

# Or manually:
make run-bg    # Start in background
make url       # Get JupyterLab URL with access token
```

### Available Commands

- `make help` - Show all available commands
- `make dev` - Quick start: build and run in background
- `make run-bg` - Start JupyterLab in background
- `make url` - Get JupyterLab URL with access token
- `make logs` - View container logs
- `make shell` - Open bash shell in container
- `make stop` - Stop the container
- `make clean` - Remove Docker image and containers

For more details, run `make help`.

