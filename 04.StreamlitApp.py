# -*- coding: utf-8 -*-
"""
Created on Tue Jul  8 12:09:20 2025

@author: risma
"""
import os
# os.environ['GDAL_DATA'] = r"C:\Users\risma\anaconda3\envs\streamlit_env\Library\share\gdal"
import matplotlib.pyplot as plt

import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
from datetime import datetime, time
import pytz
import geopandas as gpd
import folium
from streamlit_folium import st_folium
import cartopy.crs as ccrs
import cartopy.feature as cfeature

def load_data_from_duckdb(start_date, start_hour, end_date, end_hour):
    con = duckdb.connect('/home/ubuntu/weather_project/03.Outputs/NO_openWeatherData_API.duckdb')
    
    # Format dates as ISO strings to use in SQL query
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()
    
    query = f"""
    SELECT 
    name,
    Date_LTC,
    Hour_LTC,
    AVG(Temperature) AS Temperature,
    AVG(Pressure) AS Pressure,
    AVG(Humidity) AS Humidity,
    AVG(Wind_speed) AS Wind_speed,
    AVG(Wind_Deg) AS Wind_Deg,
    AVG(Rain_1h) AS Rain_1h,
    AVG(lat_api) AS lat_api,
    AVG(lon_api) AS lon_api,
    MIN(Weather_description) AS Weather_description
    FROM NOweather_data
    WHERE
    (Date_LTC > DATE '{start_date_str}' OR (Date_LTC = DATE '{start_date_str}' AND Hour_LTC >= {start_hour}))
    AND
    (Date_LTC < DATE '{end_date_str}' OR (Date_LTC = DATE '{end_date_str}' AND Hour_LTC <= {end_hour}))
    GROUP BY name, Date_LTC, Hour_LTC
    ORDER BY name, Date_LTC, Hour_LTC
    """
    
    df = con.execute(query).fetchdf()
    con.close()
    return df

# --- SIDEBAR ---
st.sidebar.title("Filters")

start_date = st.sidebar.date_input("Start Date", datetime(2025, 6, 12).date())
start_hour = st.sidebar.number_input("Start Hour", min_value=0, max_value=23, value=10)

end_date = st.sidebar.date_input("End Date", datetime(2025, 7, 3).date())
end_hour = st.sidebar.number_input("End Hour", min_value=0, max_value=23, value=10)

# --- LOAD DATA ---
df = load_data_from_duckdb(start_date, start_hour, end_date, end_hour)

st.subheader("Filtered Weather Data")
st.write(df)

if not df.empty:
    st.success(f"{df.shape[0]} rows loaded.")
else:
    st.info("No data available for selected period.")
#%%%
bar_df = df.groupby(['Date_LTC', 'Hour_LTC'])['name'].nunique().reset_index(name='reporting_stations')
bar_df['datetime'] = pd.to_datetime(bar_df['Date_LTC'].astype(str) + ' ' + bar_df['Hour_LTC'].astype(str) + ':00')

st.subheader(" Number of Unique Stations Reporting per Hour per Day")
fig_bar = px.bar(
    bar_df,
    x='datetime',
    y='reporting_stations',
    labels={'datetime': 'Date & Hour', 'reporting_stations': 'No. of Stations'},
    title="Hourly Station Reports Over Time"
)
st.plotly_chart(fig_bar)

# --- HEATMAP ---
heatmap_df = df.groupby(['Date_LTC', 'Hour_LTC'])['name'].nunique().reset_index()
pivot_df = heatmap_df.pivot(index='Date_LTC', columns='Hour_LTC', values='name').fillna(0)

fig_heatmap = px.imshow(
    pivot_df,
    labels=dict(x="Hour", y="Date", color="Stations Reporting"),
    x=pivot_df.columns,
    y=pivot_df.index,
    color_continuous_scale='Blues'
)
fig_heatmap.update_layout(title='🌡️ Stations Reporting by Hour and Date', height=500)
st.plotly_chart(fig_heatmap)
#%%
# --- Load shapefile ---
shapefile_path = '/home/ubuntu/streamlit_app/gadm41_NOR_shp/gadm41_NOR_0.shp'
gdf = gpd.read_file(shapefile_path)

# --- Prepare station GeoDataFrame ---
def pandas_mode(series):
    return series.mode().iloc[0]

stations_df = df.groupby('name').agg({
    'lon_api': pandas_mode,
    'lat_api': pandas_mode
}).reset_index()

stations_gdf = gpd.GeoDataFrame(
    stations_df,
    geometry=gpd.points_from_xy(stations_df.lon_api, stations_df.lat_api),
    crs="EPSG:4326"
)

# --- Plot with Cartopy ---
fig = plt.figure(figsize=(10, 10))
ax = plt.axes(projection=ccrs.PlateCarree())

# Set extent (optional: zoom into Norway)
ax.set_extent([4, 32, 57, 72], crs=ccrs.PlateCarree())  # [lon_min, lon_max, lat_min, lat_max]

# Add background and gridlines
ax.add_feature(cfeature.LAND, facecolor='lightgray')
ax.add_feature(cfeature.OCEAN)
ax.add_feature(cfeature.BORDERS, linestyle=':')
ax.add_feature(cfeature.COASTLINE)
ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)

# Plot shapefile
gdf.to_crs("EPSG:4326").plot(ax=ax, edgecolor='black', facecolor='none', linewidth=1)

# Plot stations
stations_gdf.plot(ax=ax, color='red', markersize=30)

# Title and layout
ax.set_title("Station Locations in Norway (with Lat/Lon Grid)", fontsize=14)
plt.tight_layout()

# --- Show in Streamlit ---
st.pyplot(fig)

#%%

# --- TIME SERIES PLOTS PER STATION ---
df['datetime'] = pd.to_datetime(df['Date_LTC'].astype(str) + ' ' + df['Hour_LTC'].astype(str), errors='coerce')

st.sidebar.header("Station Filter")
station_names = df['name'].dropna().unique()
selected_station = st.sidebar.selectbox('Select a station', sorted(station_names))

station_df = df[df['name'] == selected_station].sort_values('datetime')

variables = ['Temperature', 'Pressure', 'Humidity', 'Rain_1h']

for var in variables:
    if var in station_df.columns:
        fig = px.line(
            station_df,
            x='datetime',
            y=var,
            title=f'{var.capitalize()} Time Series - {selected_station}',
            labels={'datetime': 'Time', var: var.capitalize()}
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning(f"'{var}' not found in dataset.")
