import os
import pandas as pd
import datetime
import requests
import duckdb
from zoneinfo import ZoneInfo  # Python 3.9+

# Constants
API_KEY = "30866135c6363a37d9094214a8e419eb"
LATLON_CSV_PATH = "/home/ubuntu/weather_project/01.Inputs/NO_108weatherST_latlons.csv"
OUTPUT_DIR = "/home/ubuntu/weather_project/03.Outputs"
CSV_BASENAME = "NO_108weatherST_data.parquet"
DUCKDB_PATH = "/home/ubuntu/weather_project/03.Outputs/NO_openWeatherData_API.duckdb"

# Ensure output dir exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Get today's date and timestamp in local time
today_str = datetime.date.today().isoformat()
now_hour = datetime.datetime.now(ZoneInfo("Europe/Oslo")).strftime("%Y-%m-%d %H:00")

# Load lat-lon coordinates
latlon_key = pd.read_csv(LATLON_CSV_PATH)
new_df = pd.DataFrame()

# Fetch weather data
for _, row in latlon_key.iterrows():
    lat_provided, lon_provided = row["lat"], row["lon"]
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat_provided}&lon={lon_provided}&appid={API_KEY}"

    try:
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()

        flat = pd.json_normalize(data)
        weather = pd.json_normalize(data["weather"][0]).add_prefix("weather.")
        result = pd.concat([flat.drop(columns="weather", errors="ignore"), weather], axis=1)

        # Add custom fields
        # result["datetime"] = now_hour
        result["lat_provided"] = lat_provided
        result["lon_provided"] = lon_provided

        new_df = pd.concat([new_df, result], axis=0)

    except Exception as e:
        print(f" Failed for lat={lat_provided}, lon={lon_provided}: {e}")

if new_df.empty:
    print(" Script completed — no data collected today.")
    exit()

# === Save to Parquet ===
csv_output_path = os.path.join(OUTPUT_DIR, f"{today_str}_{CSV_BASENAME}")
try:
    if os.path.exists(csv_output_path):
        existing = pd.read_parquet(csv_output_path)
        updated = pd.concat([existing, new_df], ignore_index=True)
    else:
        updated = new_df

    updated.to_parquet(csv_output_path, index=False)
    print(f" Weather data saved to Parquet: {csv_output_path}")
except Exception as e:
    print(f" Failed to save weather data to Parquet: {e}")

# === Save to DuckDB ===
# Clean/rename columns
new_df['dt_utc'] = pd.to_datetime(new_df['dt'], unit='s', utc=True)
# Convert UTC datetime to Norway local time (handles DST automatically)
new_df['dt_ltc'] = new_df['dt_utc'].dt.tz_convert('Europe/Oslo')
new_df['Date_LTC'] = new_df['dt_ltc'].dt.date
new_df['Hour_LTC'] = new_df['dt_ltc'].dt.hour
new_df = new_df.rename(columns={
    'coord.lon': 'lon_api', 'coord.lat': 'lat_api',
    'main.temp': 'Temperature', 'main.pressure': 'Pressure',
    'main.humidity': 'Humidity', 'rain.1h': 'Rain_1h',
    'wind.speed': 'Wind_speed', 'wind.deg': 'Wind_Deg',
    'wind.gust': 'Wind_Gust', 'weather.description': 'Weather_description'
})

# Required columns
req_cols = ['name','dt', 'Date_LTC','Hour_LTC','lat_provided', 'lon_provided', 'lat_api', 'lon_api',
            'Temperature', 'Pressure', 'Humidity',
            'Wind_speed', 'Wind_Deg', 'Wind_Gust',
            'Rain_1h', 'Weather_description']

new_df = new_df[[col for col in req_cols if col in new_df.columns]]  # filter existing

try:
    con = duckdb.connect(DUCKDB_PATH)
    

    con.register("weather_temp", new_df)
    con.execute("""
        CREATE TABLE IF NOT EXISTS NOweather_data AS SELECT * FROM weather_temp LIMIT 0
    """)
    con.execute("INSERT INTO NOweather_data SELECT * FROM weather_temp")

    con.close()
    print(f" Data successfully written to DuckDB: {DUCKDB_PATH}")
except Exception as e:
    print(f" Failed to write to DuckDB: {e}")
