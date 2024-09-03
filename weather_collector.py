from constant import API_KEY, STORAGE_BUCKET_NAME,CITIES_FILE
import requests
import csv
from google.cloud import storage
from datetime import datetime
import io

class getRawData:
    def __init__(self, CITIES_FILE, API_KEY, STORAGE_BUCKET_NAME):
        self.CITIES_FILE = CITIES_FILE  #self.cities_file sınıf içinde her yerden erişilebilir ve bu, cities_file parametresinin değerini tutar.
        self.API_KEY = API_KEY
        self.storage_client = storage.Client()    #Bu satır, Google Cloud Storage ile etkileşim kurabilmek için bir storage.Client nesnesi oluşturur ve bunu self.storage_client örnek değişkenine atar.
            #storage.Client() fonksiyonu, Google Cloud SDK tarafından sağlanan bir fonksiyondur ve belirli bir proje ve kimlik bilgileri ile GCS'ye bağlanmanızı sağlar.
        self.STORAGE_BUCKET_NAME = STORAGE_BUCKET_NAME

    def read_cities(self):
        try:
            with open(self.CITIES_FILE, 'r') as file:
                cities = file.read().splitlines()
            return cities
        except Exception as e:
            print(f"Error reading txt file: {e}")
            return[]
    
    def extract_data(self,city):
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={self.API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            data=response.json()    #Bu kod satırı, HTTP yanıtındaki JSON verisini bir Python sözlüğüne (dict) dönüştürür ve data değişkenine atar.
            return data
        else:
            print(f"Error fetching data for {city}: {response.status_code} - {response.text}")

    def load_to_gcs(self, data):
        # CSV formatında veri oluştur

        # Dinamik olarak CSV başlıklarını (fieldnames) belirle
        fieldnames = set()
        for entry in data:
            fieldnames.update(entry.keys())
        
        fieldnames = sorted(fieldnames)  # Başlıkları sıralamak düzenli bir CSV dosyası sağlar
        
        output = io.StringIO()  # Veriyi hafızada tutmak için bir StringIO nesnesi oluşturuyoruz.
        writer = csv.DictWriter(output, fieldnames=fieldnames)  # CSV yazıcısı oluşturuluyor.
        writer.writeheader()  # CSV dosyasına başlık satırını yazıyor.
        writer.writerows(data)  # Veriyi satır satır yazıyor.
        
        output.seek(0)  # Verinin başına dönüyor.
        
        bucket = self.storage_client.bucket(self.STORAGE_BUCKET_NAME)  # İlgili bucket'a erişiyoruz.
        blob = bucket.blob(f'raw_weather_data_{datetime.utcnow().isoformat()}.csv')  # Yeni bir blob oluşturuyoruz.
        blob.upload_from_string(output.getvalue(), content_type='text/csv')  # CSV verisini blob'a yüklüyoruz.
        
        print(f'Data loaded to GCS: {blob.name}')  # Yüklenen dosyanın adını ekrana yazdırıyoruz.




    def run_ext(self):
        cities = self.read_cities()
        all_data = []  # Tüm veriyi tutmak için bir liste

        for city in cities:
            raw_data=self.extract_data(city)
            if raw_data:
                all_data.append(raw_data)  # Her bir şehirden gelen veriyi listeye ekle
            else:
                print("error")
        
        if all_data:
            self.load_to_gcs(all_data)  # Tüm veriyi GCS'ye yükle
        else:
            print("No data to load.")


