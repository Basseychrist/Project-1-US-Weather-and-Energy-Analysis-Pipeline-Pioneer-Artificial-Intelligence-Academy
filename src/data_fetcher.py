import requests
import pandas as pd
import time
import logging
import os
import yaml
from datetime import datetime, timedelta

logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def fetch_noaa_weather_data(api_token, cities, start_date, end_date, max_retries=5, backoff_factor=2):
    """
    Fetch weather data (TMAX, TMIN) from NOAA API for multiple cities.
    """
    base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    headers = {"token": api_token}
    all_records = []
    for city in cities:
        for datatype in ["TMAX", "TMIN"]:
            params = {
                "datasetid": "GHCND",
                "startdate": start_date,
                "enddate": end_date,
                "limit": 1000,
                "units": "metric",
                "datatypeid": datatype,
                "stationid": city["noaa_station_id"],
                "includemetadata": "false",
                "sortfield": "date",
                "sortorder": "asc"
            }
            attempt = 0
            while attempt <= max_retries:
                try:
                    response = requests.get(base_url, headers=headers, params=params, timeout=30)
                    if response.status_code == 429:
                        wait = backoff_factor ** attempt
                        logging.warning(f"NOAA rate limited for {city['name']} ({datatype}). Retrying in {wait}s...")
                        time.sleep(wait)
                        attempt += 1
                        continue
                    response.raise_for_status()
                    data = response.json().get("results", [])
                    for record in data:
                        record["city"] = city["name"]
                        record["state"] = city["state"]
                        record["datatype"] = datatype
                        all_records.append(record)
                    break
                except Exception as e:
                    logging.error(f"NOAA error for {city['name']} ({datatype}): {e}")
                    wait = backoff_factor ** attempt
                    time.sleep(wait)
                    attempt += 1
            else:
                logging.error(f"NOAA failed for {city['name']} ({datatype}) after {max_retries} retries.")
    if all_records:
        df = pd.json_normalize(all_records)
        df = df.rename(columns={
            "date": "date",
            "datatype": "datatype",
            "value": "value",
            "city": "city",
            "state": "state"
        })
        df = df[["city", "state", "date", "datatype", "value"]]
        return df
    else:
        return pd.DataFrame(columns=["city", "state", "date", "datatype", "value"])

def fetch_eia_energy_data(api_key, regions, start_date, end_date, max_retries=5, backoff_factor=2):
    """
    Fetch energy consumption data from EIA API for given regions.
    """
    base_url = "https://api.eia.gov/v2/electricity/rto/region-data/data/"
    all_records = []
    for region in regions:
        params = {
            "api_key": api_key,
            "frequency": "hourly",
            "data[0]": "value",
            "facets[respondent][]": region["eia_region_code"],
            "start": start_date,
            "end": end_date
        }
        attempt = 0
        while attempt <= max_retries:
            try:
                response = requests.get(base_url, params=params, timeout=10)
                if response.status_code == 429:
                    wait = backoff_factor ** attempt
                    logging.warning(f"EIA rate limited for {region['name']}. Retrying in {wait}s...")
                    time.sleep(wait)
                    attempt += 1
                    continue
                if response.status_code != 200:
                    logging.error(f"EIA error for {region['name']}: {response.status_code} {response.reason} - {response.text}")
                    raise Exception(f"EIA API error: {response.status_code} {response.reason}")
                data = response.json().get("response", {}).get("data", [])
                for record in data:
                    record["region"] = region["name"]
                    record["state"] = region["state"]
                    all_records.append(record)
                break
            except Exception as e:
                logging.error(f"EIA error for {region['name']}: {e}")
                wait = backoff_factor ** attempt
                time.sleep(wait)
                attempt += 1
        else:
            logging.error(f"EIA failed for {region['name']} after {max_retries} retries.")
    if all_records:
        df = pd.json_normalize(all_records)
        date_col = "period" if "period" in df.columns else "date"
        value_col = "value" if "value" in df.columns else df.columns[-1]
        df = df.rename(columns={date_col: "date", value_col: "value", "region": "region", "state": "state"})
        df = df[["region", "state", "date", "value"]]
        return df
    else:
        return pd.DataFrame(columns=["region", "state", "date", "value"])

def get_90_days_range():
    end_date = datetime.today()
    start_date = end_date - timedelta(days=89)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def main():
    logging.info("Pipeline started")
    # Load config.yaml
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    noaa_api_key = config.get('noaa_api_key')
    eia_api_key = config.get('eia_api_key')

    # City Reference Table
    cities = [
        {"name": "New York", "state": "New York", "noaa_station_id": "GHCND:USW00094728", "eia_region_code": "NYIS"},
        {"name": "Chicago", "state": "Illinois", "noaa_station_id": "GHCND:USW00094846", "eia_region_code": "PJM"},
        {"name": "Houston", "state": "Texas", "noaa_station_id": "GHCND:USW00012960", "eia_region_code": "ERCO"},
        {"name": "Phoenix", "state": "Arizona", "noaa_station_id": "GHCND:USW00023183", "eia_region_code": "AZPS"},
        {"name": "Seattle", "state": "Washington", "noaa_station_id": "GHCND:USW00024233", "eia_region_code": "SCL"}
    ]
    # For EIA, use only region and state
    regions = [{"name": c["name"], "state": c["state"], "eia_region_code": c["eia_region_code"]} for c in cities]

    start_date, end_date = get_90_days_range()

    # Fetch weather data (TMAX/TMIN)
    try:
        weather_df = fetch_noaa_weather_data(noaa_api_key, cities, start_date, end_date)
        logging.info("NOAA weather data fetched successfully.")
    except Exception as e:
        logging.error(f"Failed to fetch NOAA weather data: {e}")
        weather_df = pd.DataFrame(columns=["city", "state", "date", "datatype", "value"])

    # Fetch EIA energy data
    try:
        energy_df = fetch_eia_energy_data(eia_api_key, regions, start_date, end_date)
        logging.info("EIA energy data fetched successfully.")
    except Exception as e:
        logging.error(f"Failed to fetch EIA energy data: {e}")
        energy_df = pd.DataFrame(columns=["region", "state", "date", "value"])

    # Save to data/raw folder
    raw_folder = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
    os.makedirs(raw_folder, exist_ok=True)
    weather_path = os.path.join(raw_folder, 'noaa_weather_data.csv')
    energy_path = os.path.join(raw_folder, 'eia_energy_data.csv')
    weather_df.to_csv(weather_path, index=False)
    energy_df.to_csv(energy_path, index=False)
    print(f"Weather data saved to {weather_path}")
    print(f"Energy data saved to {energy_path}")
    print(f"Fetched {len(energy_df)} EIA records")

if __name__ == "__main__":
    main()