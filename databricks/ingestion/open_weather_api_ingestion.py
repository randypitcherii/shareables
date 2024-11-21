import json
import requests
from datetime import datetime
import uuid

# Config
TARGET_VOLUME = 'randy_pitcher_workspace.raw.weather_json'
OPEN_WEATHER_API_KEY = dbutils.secrets.get(scope="randy_pitcher_workspace", key="open_weather_api_key")
CITIES = [
    "Indianapolis", 
    "Nashville", 
    "San Jose", 
    "St. Louis", 
    "New York", 
    "London", 
    "Paris", 
    "Tokyo", 
    "Sydney",
    "Chicago",
    "Porto",
]


def get_weather(city, api_key):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "imperial",  # use 'metric' for Celsius
    }
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred for city {city}: {http_err}")
        return None
    except Exception as err:
        print(f"Other error occurred for city {city}: {err}")
        return None
    else:
        return response.json()


def main():
    # create volume for storing files if it doesn't exist
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {TARGET_VOLUME}          
    """)
    
    for city in CITIES:
        weather_data = get_weather(city, OPEN_WEATHER_API_KEY)

        if weather_data:
            file_name = f"{city}--{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}--{uuid.uuid4()}.json" 
            print(f"Processing {file_name} to {TARGET_VOLUME}/{file_name}")
            dbutils.fs.put(f"/Volumes/{TARGET_VOLUME.replace('.', '/')}/{file_name}", json.dumps(weather_data), overwrite=True)

# Run the main function
main()