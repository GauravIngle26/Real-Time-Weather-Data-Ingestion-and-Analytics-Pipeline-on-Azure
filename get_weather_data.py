#Import required libraries.
import requests
import json
import time
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData

# OpenWeatherMap API setup
api_key = "<personal API key from OpenWeatherMap account>"
city = "Munich"
url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

# Azure Event Hub setup
event_hub_connection_str = "<event hub connection string from Azure subscription>" #put here the connection string of event hub from azure.
event_hub_name = "<event hub name>" #put here your event hub name created on azure

# Function to fetch weather data from OpenWeatherMap API:
def fetch_weather_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        print('The API is working!')
        data = response.json()
        return data
    else:
        print(f"Failed to fetch data from OpenWeatherMap API: {response.status_code}")
        return None

# Function to preprocess the fetched data:
def preprocess_data(data):
    # Extract and flatten the JSON data
    flattened_data = {
        'Longitude': data['coord']['lon'],
        'Latitude': data['coord']['lat'],
        'weather_id': data['weather'][0]['id'],
        'weather_main': data['weather'][0]['main'],
        'weather_description': data['weather'][0]['description'],
        'weather_icon': data['weather'][0]['icon'],
        'int_par_base': data['base'],
        'visibility': data['visibility'],
        'time_data_cal_unix': data['dt'],
        'timezone': data['timezone'],
        'city_id': data['id'],
        'city_name': data['name'],
        'int_par_cod': data['cod'],
        'temp': data['main']['temp'],
        'temp_feels_like': data['main']['feels_like'],
        'temp_min': data['main']['temp_min'],
        'temp_max': data['main']['temp_max'],
        'pressure': data['main']['pressure'],
        'humidity': data['main']['humidity'],
        'sea_level': data['main'].get('sea_level'),
        'grnd_level': data['main'].get('grnd_level'),
        'wind_speed': data['wind']['speed'],
        'wind_deg': data['wind']['deg'],
        'wind_gust': data['wind'].get('gust'),
        'cloudiness_percentage': data['clouds']['all'],
        'sys_type': data['sys'].get('type'),
        'int_sys_id': data['sys']['id'],
        'int_sys_country_code': data['sys']['country'],
        'int_sys_sunrise_unix': data['sys']['sunrise'],
        'int_sys_sunset_unix': data['sys']['sunset'],
    }

    # Convert to DataFrame
    df = pd.DataFrame([flattened_data])
    
    # Select desired columns
    selected_columns = [
        'Longitude', 'Latitude', 'weather_id', 'weather_main', 'weather_description', 'weather_icon',
        'int_par_base', 'visibility', 'time_data_cal_unix', 'timezone', 'city_id', 'city_name', 
        'int_par_cod', 'temp', 'temp_feels_like', 'temp_min', 'temp_max', 'pressure', 'humidity', 
        'sea_level', 'grnd_level', 'wind_speed', 'wind_deg', 'wind_gust', 'cloudiness_percentage', 
        'sys_type', 'int_sys_id', 'int_sys_country_code', 'int_sys_sunrise_unix', 'int_sys_sunset_unix'
    ]
    df = df[selected_columns]

    # Testing the data:
    #df.to_csv('fetch_weather_data.csv', header=True)
    #print(df)
    
    # Convert DataFrame to JSON string
    json_data = df.to_json(orient='records')
    return json_data

#Function to send the data to event hub batchwise:
def send_to_event_hub(data):
    if data is not None:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=event_hub_connection_str,
            eventhub_name=event_hub_name
        )
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(data))
        producer.send_batch(event_data_batch)
        producer.close()
        print('Data sent to Event Hub successfully!')
    else:
        print('No data to send to Event Hub.')

if __name__ == "__main__":
    while True:
        weather_data = fetch_weather_data(url)
        if weather_data:
            processed_data = preprocess_data(weather_data)
            send_to_event_hub(processed_data)
        time.sleep(120)  # Fetch data every 2 minutes and send it to Azure event hub.
