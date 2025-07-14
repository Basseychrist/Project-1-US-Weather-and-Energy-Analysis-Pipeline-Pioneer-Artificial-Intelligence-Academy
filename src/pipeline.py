import os
import logging


logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)
# Set up logging first

from data_fetcher import fetch_noaa_weather_data, fetch_eia_energy_data  # Now import other modules

def main():
    logging.info("Pipeline started")
    fetch_noaa_weather_data()
    fetch_eia_energy_data()
    logging.info("Pipeline completed successfully")
if __name__ == "__main__":
    main() 