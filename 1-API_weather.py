# libraries import

import pandas as pd 
import requests
import json
from pandas import json_normalize
import time
from datetime import datetime
import plotly.io as pio
import plotly.express as px
import plotly.graph_objects as go
from typing import Any, Dict, List
import boto3
import os

# city list given by the exercice

city_list = ["Mont Saint Michel",
"St Malo",
"Bayeux",
"Le Havre",
"Rouen",
"Paris",
"Amiens",
"Lille",
"Strasbourg",
"Chateau du Haut Koenigsbourg",
"Colmar",
"Eguisheim",
"Besancon",
"Dijon",
"Annecy",
"Grenoble",
"Lyon",
"Gorges du Verdon",
"Bormes les Mimosas",
"Cassis",
"Marseille",
"Aix en Provence",
"Avignon",
"Uzes",
"Nimes",
"Aigues Mortes",
"Saintes Maries de la mer",
"Collioure",
"Carcassonne",
"Ariege",
"Toulouse",
"Montauban",
"Biarritz",
"Bayonne",
"La Rochelle"]

# request for lat & long for each city

position_cities = {}

for city in city_list:
    resp =  requests.get(f"https://nominatim.openstreetmap.org/?q={city}&format=json&countrycodes=1974&limit=1").json()
    lat = resp[0]['lat']
    long = resp[0]['lon']
    position_cities[city] = [lat,long]

position_cities

# request for the weather for each city 

def get_weather_forecasts(latitude, longitude):
    resp =  requests.get(f"http://api.openweathermap.org/data/2.5/forecast?lat={latitude}&lon={longitude}&appid=c57023819bfd474995f3bf1b69e50c0d")
    resp.raise_for_status()
    return resp.json()['list']

# Keep usefull information, in our case I choose the precipiration & the temperature

chosen_weather = []

for city, (latitude, longitude) in position_cities.items():
    weather_forecasts = get_weather_forecasts(latitude, longitude)

    for forecast in weather_forecasts: 
        dt_object = datetime.utcfromtimestamp(forecast['dt'])
        main_weather = forecast['weather'][0]['main']
        prepcipitation = forecast['pop']
        temperature = forecast['main']['temp'] - 273.15 # convertion K to C
    
        chosen_weather.append({'city' : city,
                        'dt_object': dt_object,
                        'latitude': latitude,
                        'longitude': longitude,
                        'main_weather': main_weather, 
                        'prepcipitation' :prepcipitation,
                        'temperature' :temperature})

chosen_weather

# we tranform our data in dataframe

city_weather = pd.DataFrame.from_records(chosen_weather)
city_weather

# tranform the infomation about city weather by days and not anymore by each 3h

city_weather['latitude'] = city_weather['latitude'].astype(float)
city_weather['longitude'] = city_weather['longitude'].astype(float)

city_weather_by_day = city_weather.groupby([city_weather['city'],city_weather['dt_object'].dt.date]).agg({ 
    'main_weather' : [pd.Series.mode], 
    'prepcipitation' : ['sum'], 
    'temperature' : ['mean'],
    'latitude' : ['mean'],
    'longitude' : ['mean']}).reset_index()

city_weather_by_day.columns = city_weather_by_day.columns.droplevel(1)

# send the data in our S3

session = boto3.Session(aws_access_key_id=os.getenv("AWS_ACCESS_LEY_ID"), 
                        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))


s3 = session.resource("s3")

bucket = s3.create_bucket(Bucket="booking-scapping")

csv = city_weather_by_day.to_csv()

put_object = bucket.put_object(Key="city_weather_by_day.csv", Body=csv)

# creation of a file with the top 5 to feed the booking scrapping

top_city = city_weather_by_day.groupby('city', sort=False).mean().reset_index().sort_values(['prepcipitation','temperature'], ascending = [True, False])

Top_city = top_city['city']
Top_5_city = Top_city.head(5)

path = 'data/'
os.chdir(path)

Top_5_city.to_csv('top_5_city_name.csv', index=False)

print('We got the top 5 cities in our data folder !')