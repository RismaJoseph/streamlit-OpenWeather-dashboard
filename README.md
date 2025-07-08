
# Norway Weather Data ETL Pipeline

This project builds a simple yet robust ETL pipeline to fetch, process, and visualize **hourly weather data** from [OpenWeatherMap](https://api.openweathermap.org/) for selected locations across Norway. It leverages **DuckDB** for fast local querying, **Parquet** for efficient storage, and **AWS S3** for backups — all hosted on an **EC2 instance** with a Streamlit dashboard.



# Project Overview

* **Location Source**: Latitude and longitude coordinates are selected from [frost.met.no](https://frost.met.no/index.html).
* **Geospatial Data**: Norway’s shapefile is sourced from the [EEA](https://www.eea.europa.eu/data-and-maps/data/eea-reference-grids-2/gis-files/norway-shapefile).
* **Weather Data**: Data is fetched hourly via the [OpenWeatherMap API](https://api.openweathermap.org/).



# ETL Workflow

1. **Hourly Fetch**:

   * Fetches weather data for all selected lat/lon pairs every hour.
   * If nearby coordinates are nearly identical, their data (pressure, wind, etc.) is averaged to reduce redundancy.
   * Timestamps in UTC are converted to local Norwegian time (CET/CEST).

2. **Storage**:

   * Appended hourly to a **DuckDB** file for fast querying.
   * Written daily to **Parquet**, uploaded to **AWS S3**, then deleted from EC2 to save disk space.
   * Full DuckDB backups are pushed to S3 weekly.

3. **Visualization**:

   * A **Streamlit dashboard** hosted on EC2 uses the DuckDB file to:

     * Filter data by date and hour
     * Filter by individual station
     * Plot time series for temperature, pressure, etc.
     * Show spatial distribution of reporting stations
     * Show count of stations reporting per hour



# Cron Job Scheduling

Weather data is fetched hourly using `cron`. Example cron job:

```bash
crontab -e
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

0 * * * * /home/ubuntu/weather_project/02.Codes/weather_wrapper.sh >> /home/ubuntu/weather_project/03.Outputs/cron_log_v2.txt 2>&1
20 1 * * * /home/ubuntu/weather_project/02.Codes/OpenWeather_upload_to_s3_v2.sh >> /home/ubuntu/weather_project/03.Outputs/upload_log_v2.txt 2>&1
20 1 * * 0 /home/ubuntu/weather_project/02.Codes/duckdb_toS3.sh >> /home/ubuntu/weather_project/03.Outputs/duckdbupload_log.txt 2>&1


#Tech Stack

* **Python**
* **DuckDB**
* **Streamlit**
* **AWS S3**
* **OpenWeatherMap API**
* **GeoPandas** (for shapefile processing)
* **cron** (scheduling)


# Notes & Limitations

* A slight time lag of ±5 minutes may occur during each fetch due to API/network delays.
* Timezone conversion from UTC → CET/CEST is handled correctly.
* Data is updated hourly, and a 30-day window is currently retained for visualization.
* What happens if the OpenWeatherMap API call fails (rate-limiting, network issue)?


# Resources

*  [Frost.met.no](https://frost.met.no/index.html)
*  [EEA Norway Shapefile](https://www.eea.europa.eu/data-and-maps/data/eea-reference-grids-2/gis-files/norway-shapefile)
*  [OpenWeatherMap API](https://api.openweathermap.org/)
