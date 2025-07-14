import requests
import pandas as pd
import time
import logging

def fetch_noaa_weather_data(api_token, cities, datasetid, start_date, end_date, max_retries=5, backoff_factor=2):
    """
    Fetch weather data from NOAA API for multiple cities with rate limiting and exponential backoff.
    
    Args:
        api_token (str): NOAA API token.
        cities (list of dict): Each dict should have 'name', 'lat', 'lon'.
        datasetid (str): NOAA dataset ID (e.g., 'GHCND').
        start_date (str): Start date in 'YYYY-MM-DD'.
        end_date (str): End date in 'YYYY-MM-DD'.
        max_retries (int): Max number of retries per request.
        backoff_factor (int): Exponential backoff multiplier.
    
    Returns:
        pd.DataFrame: Combined weather data for all cities.
    """
    base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    headers = {"token": api_token}
    all_records = []
    for city in cities:
        params = {
            "datasetid": datasetid,
            "startdate": start_date,
            "enddate": end_date,
            "limit": 1000,
            "units": "metric",
            "datatypeid": "TAVG",  # Example: average temperature
            "locationcategoryid": "CITY",
            "includemetadata": "false",
            "sortfield": "date",
            "sortorder": "asc"
        }
        # Optionally, use city['lat'], city['lon'] to filter by location
        params["locationid"] = city.get("locationid")  # If you have NOAA location IDs
        attempt = 0
        while attempt <= max_retries:
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=10)
                if response.status_code == 429:  # Rate limit
                    wait = backoff_factor ** attempt
                    logging.warning(f"Rate limited for {city['name']}. Retrying in {wait}s...")
                    time.sleep(wait)
                    attempt += 1
                    continue
                response.raise_for_status()
                data = response.json().get("results", [])
                for record in data:
                    record["city"] = city["name"]
                    all_records.append(record)
                break  # Success, exit retry loop
            except Exception as e:
                logging.error(f"Error fetching data for {city['name']}: {e}")
                wait = backoff_factor ** attempt
                time.sleep(wait)
                attempt += 1
        else:
            logging.error(f"Failed to fetch data for {city['name']} after {max_retries} retries.")
    # Normalize and return DataFrame with consistent columns
    if all_records:
        df = pd.json_normalize(all_records)
        # Rename columns for consistency
        df = df.rename(columns={
            "date": "date",
            "datatype": "datatype",
            "value": "value",
            "city": "city"
        })
        df = df[["city", "date", "datatype", "value"]]
        return df
    else:
        return pd.DataFrame(columns=["city", "date", "datatype", "value"])

def main():
    print("Data fetcher running!")