# US Weather + Energy Analysis Pipeline

## Overview

This project builds a production-ready data pipeline that combines weather and energy consumption data for 5 major US cities. The goal is to help utilities optimize power generation, reduce waste, and lower costs through better demand forecasting.

## Business Context

Energy companies lose millions of dollars due to inaccurate demand forecasting. By combining weather data with energy consumption patterns, data scientists can help utilities optimize power generation, reduce waste, and lower costs. This project demonstrates ability to build production-ready data pipelines that deliver real business value.

## Features

- **Automated Data Pipeline:** Fetches daily weather (NOAA) and energy (EIA) data for New York, Chicago, Houston, Phoenix, and Seattle.
- **Data Quality Reporting:** Automated checks for missing values, outliers, and data freshness.
- **Interactive Dashboard:** Streamlit app with geographic, time series, correlation, and heatmap visualizations.
- **Production-Ready Code:** Modular Python modules, config-driven, robust logging, and clear documentation.

## Repository Structure

```
├── README.md                 # Business-focused project summary
├── AI_USAGE.md               # AI assistance documentation
├── pyproject.toml            # Dependencies (using uv)
├── config/
│   └── config.yaml           # API keys, cities list
├── src/
│   ├── data_fetcher.py       # API interaction module
│   ├── data_processor.py     # Cleaning and transformation
│   ├── analysis.py           # Statistical analysis
│   └── pipeline.py           # Main orchestration
├── dashboards/
│   └── app.py                # Streamlit application
├── logs/
│   └── pipeline.log          # Execution logs
├── data/
│   ├── raw/                  # Original API responses
│   └── processed/            # Clean, analysis-ready data
├── notebooks/
│   └── exploration.ipynb     # Initial analysis (optional)
├── tests/
│   └── test_pipeline.py      # Basic unit tests
└── video_link.md             # Link to my presentation
```

## Setup Instructions

1. **Clone the repository**

   ```sh
   git clone <your-repo-url>
   cd Project-1-US-Weather-and-Energy-Analysis-Pipeline-Pioneer-Artificial-Intelligence-Academy
   ```

2. **Install dependencies**

    # to set-up , use:

    # On Git Bash on Windows, use:
    ```sh
    source .venv/Scripts/activate
    ```

    # On Command Prompt (cmd.exe), use:
    .venv\Scripts\activate
    

    # On macOS/Linux:
    source .venv/bin/activate


    # dependencies manager
    pip 

    
    # to install dependencies
    pip install .

    
    # Using developer Tools
    **Format code:** black src/

    **Sort imports:** isort src/
    
    **Run tests:** pytest
    
    **Check types:** mypy src/

    **Lint:** ruff src/

    # Install Dev Tools (Optional but Recommended)

    **Install dev tools with:**  pip install .[dev]

- Now your tools and dependencies are managed in one place.





3. **Configure API keys and cities**

   - Edit `config/config.yaml` with your NOAA and EIA API keys and city list.

4. **Run the pipeline**

   ```sh
      run-pipeline
   ```

5. **Launch the dashboard**
   ```sh
   streamlit run dashboards/app.py

6. **fetch the data**
   ```sh
   data_fetcher
   ```

## Data Sources

- **NOAA Climate Data Online:** [API Docs](https://www.ncei.noaa.gov/cdo-web/api/v2)
- **EIA Energy API:** [API Docs](https://api.eia.gov/v2/electricity/)

## Registration Requirements:

- **NOAA Climate Data Online:** Register at https://www.ncdc.noaa.gov/cdo-web/token
- **EIA Energy API:** Register at https://www.eia.gov/opendata/register.php

## Documentation

- **AI Usage:** See `AI_USAGE.md` for details on AI-assisted development.
- **Testing:** Run `pytest tests/` to execute unit tests.

## Troubleshooting

- See the README and comments in each module for common issues and solutions.
- Logs are written to `logs/pipeline.log`.

## License

MIT License

---

\*For more details, see each module and the AI_USAGE.md file.\*
