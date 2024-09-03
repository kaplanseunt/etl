from constant import API_KEY, STORAGE_BUCKET_NAME,CITIES_FILE
import requests
import csv
from google.cloud import storage
from datetime import datetime
import io

class TransformationLoad:
    def __init__(self, CITIES_FILE, API_KEY, STORAGE_BUCKET_NAME):
        self.CITIES_FILE = CITIES_FILE
        self.API_KEY = API_KEY
        self.storage_client = storage.Client()
        self.STORAGE_BUCKET_NAME = STORAGE_BUCKET_NAME

    def read_cities(self):
        try:
            with open(self.CITIES_FILE, 'r') as file:
                cities = file.read().splitlines()
            return cities
        except Exception as e:
            print(f"Error reading cities file: {e}")
            return []   #raise error logu koy

    def extract(self, city):
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={self.API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json() 
            return data
        else:
            print(f"Error fetching data for {city}: {response.status_code} - {response.text}")
            return None

    def kelvin_to_celsius(self,kelvin):
        return round(kelvin-273.15)
    def unix_to_datetime(self,unix_timestamp):
        dt=datetime.utcfromtimestamp(unix_timestamp)
        return dt.strftime('%Y-%m-%d %H:%M')

    def transform(self,data):
        if data is None:
            return None

        humidity = data['main'].get('humidity')
        if humidity < 30:
            humidity_status = 'dry'
        elif 30 <= humidity <= 60:
            humidity_status = 'normal'
        else:
            humidity_status = 'humid'

        return {        #Bu yapı bir Python sözlüğüdür (dict). Python'da sözlükler {} süslü parantezlerle tanımlanır ve anahtar-değer (key-value) çiftlerinden oluşur.
            'timestamp': datetime.utcnow().isoformat(),
            'location': data.get('name'),
            'temperature': f"{self.kelvin_to_celsius(data['main'].get('temp'))}C",
            'description': data['weather'][0].get('description'),
            'longitude': data['coord'].get('lon'),
            'latitude': data['coord'].get('lat'),
            'feels_like': f"{self.kelvin_to_celsius(data['main'].get('feels_like'))}C",
            'pressure': data['main'].get('pressure'),
            'humidity': data['main'].get('humidity'),
            'humidity_status': humidity_status,  # Nem durumunu ekledik
            'visibility': data['visibility'],
            'wind_speed': data['wind'].get('speed'),
            'time': self.unix_to_datetime(data['dt']),
            'country': data['sys'].get('country'),
            'sunrise': self.unix_to_datetime(data['sys'].get('sunrise')),
            'sunset': self.unix_to_datetime(data['sys'].get('sunset'))
        }

    def load_to_gcs(self, data):
        # CSV formatında veri oluştur
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        
        output.seek(0)
        
        bucket = self.storage_client.bucket(self.STORAGE_BUCKET_NAME)
        blob = bucket.blob(f'transformed_weather_data_{datetime.utcnow().isoformat()}.csv')
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        
        print(f'Data loaded to GCS: {blob.name}')
        self.latest_file_name = blob.name  # Burada örnek değişkenini güncelliyoruz


    def run_etl(self):
        cities = self.read_cities()
        all_transformed_data = []
        for city in cities:
            raw_data = self.extract(city)
            transformed_data = self.transform(raw_data)
            if transformed_data:
                all_transformed_data.append(transformed_data)
        
        if all_transformed_data:
            #print(all_transformed_data)
            self.load_to_gcs(all_transformed_data)
        else:
            print("No data to load.")
        
        return self.latest_file_name        


