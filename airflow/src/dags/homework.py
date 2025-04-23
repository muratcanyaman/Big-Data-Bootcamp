from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
import requests

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from datetime import datetime, timedelta
import psycopg2

# A DAG represents a workflow, a collection of tasks
with DAG(
    dag_id="homework",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    schedule_interval="*/5 * * * *") as dag:  # Her 5 dakikada bir çalışacak şekilde ayarlandı

    client = MongoClient("mongodb+srv://cetingokhan:cetingokhan@cluster0.ff5aw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")


    def generate_random_heat_and_humidity_data(dummy_record_count:int):
        import random
        import datetime
        try:
            from models.heat_and_humidity import HeatAndHumidityMeasureEvent
            print("Model başarıyla import edildi.")
        except ImportError as e:
            print(f"Model import hatası: {str(e)}")
            # Alternatif olarak burada basit bir model sınıfı oluşturun
            class HeatAndHumidityMeasureEvent():
                def __init__(self, temperature, humidity, timestamp, creator):
                    self.temperature = temperature
                    self.humidity = humidity
                    self.timestamp = timestamp
                    self.creator = creator
            print("Yedek model sınıfı oluşturuldu.")
        
        records = []
        for i in range(dummy_record_count):
            temperature = random.randint(10, 40)
            humidity = random.randint(10, 100)
            timestamp = datetime.datetime.now()
            creator = "Murat Can Yaman"
            record = HeatAndHumidityMeasureEvent(temperature, humidity, timestamp, creator)
            records.append(record)
        return records
    
    def save_data_to_mongodb(records):
        db = client["bigdata_training"]
        collection = db["user_coll_MuratCanYaman"]  # Kendi adınızı içeren collection adı
        for record in records:
            collection.insert_one(record.__dict__)


    def create_sample_data_on_mongodb():
        try:
            # MongoDB bağlantısını test et
            print("MongoDB bağlantısı test ediliyor...")
            client.admin.command('ping')
            print("MongoDB bağlantısı başarılı!")
            
            records = generate_random_heat_and_humidity_data(10)
            save_data_to_mongodb(records)
            print(f"{len(records)} adet sıcaklık ve nem verisi MongoDB'ye kaydedildi.")
            # Nesne listesi yerine basit bir değer döndür
            return {"status": "success", "records_count": len(records)}
        except Exception as e:
            print(f"Veri oluşturma veya kaydetme sırasında hata: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
        

    def copy_anomalies_into_new_collection():        
        # sample_coll collectionundan temperature 30 dan büyük olanları new(kendi adınıza bir collectionname) 
        # collectionuna kopyalayın(kendi creatorunuzu ekleyin)
        from models.heat_and_humidity import HeatAndHumidityMeasureEvent
        
        db = client["bigdata_training"]
        source_collection = db["user_coll_MuratCanYaman"]
        target_collection = db["user_coll_anomalies_MuratCanYaman"]  # Kendi adınızı içeren koleksiyon adı
        
        # 30 dereceden yüksek sıcaklık kayıtlarını bul
        anomaly_records = source_collection.find({"temperature": {"$gt": 30}})
        
        # Her bir kaydı yeni koleksiyona ekle, kendi creator bilgini ekleyerek
        count = 0
        for record in anomaly_records:
            # Creator bilgisini güncelle
            record["creator"] = "Murat Can Yaman"
            # _id alanını kaldır, MongoDB yeni bir _id oluşturacak
            if "_id" in record:
                del record["_id"]
            # Yeni koleksiyona ekle
            target_collection.insert_one(record)
            count += 1
            
        print(f"{count} adet anomali kaydı 'user_coll_anomalies_MuratCanYaman' koleksiyonuna kopyalandı.")
        

    def copy_airflow_logs_into_new_collection():            
        conn = psycopg2.connect(
            host="postgres",
            port="5432",
            database="airflow",
            user="airflow",
            password="airflow"
        )

        # airflow veritababnındaki log tablosunda bulunan verilerin son 1 dakikasında oluşan event bazındaki kayıt sayısını 
        # mongo veritabanında oluşturacağınız"log_adınız" collectionına event adı ve kayıt sayısı bilgisi ile 
        # birlikte(güncel tarih alanına ekleyerek) yeni bir tabloya kaydedin.
        # Örn çıktı;
        #{
        #    "event_name": "task_started",
        #    "record_count": 10,
        #    "created_at": "2022-01-01 00:00:00"
        #}
        
        cursor = conn.cursor()
        
        # Son 1 dakika içinde oluşan logları sorgula
        one_minute_ago = datetime.now() - timedelta(minutes=1)
        query = """
        SELECT event, COUNT(*) as count
        FROM log
        WHERE dttm >= %s
        GROUP BY event
        """
        cursor.execute(query, (one_minute_ago,))
        
        # MongoDB'ye kaydet
        db = client["bigdata_training"]
        log_collection = db["user_coll_logs_MuratCanYaman"]  # Kendi adınızı içeren koleksiyon adı
        
        current_time = datetime.now()
        
        # Her bir event tipini MongoDB'ye ekle
        records_added = 0
        for row in cursor.fetchall():
            event_name = row[0]
            record_count = row[1]
            
            log_data = {
                "event_name": event_name,
                "record_count": record_count,
                "created_at": current_time
            }
            
            log_collection.insert_one(log_data)
            records_added += 1
        
        cursor.close()
        conn.close()
        
        print(f"{records_added} adet log kaydı 'user_coll_logs_MuratCanYaman' koleksiyonuna eklendi.")

    # Task tanımlamaları
    dag_start = DummyOperator(task_id="start")
    
    create_sample_data_task = PythonOperator(
        task_id="create_sample_data",
        python_callable=create_sample_data_on_mongodb,
        dag=dag
    )
    
    copy_anomalies_task = PythonOperator(
        task_id="copy_anomalies_into_new_collection",
        python_callable=copy_anomalies_into_new_collection,
        dag=dag
    )
    
    insert_logs_task = PythonOperator(
        task_id="insert_airflow_logs_into_mongodb",
        python_callable=copy_airflow_logs_into_new_collection,
        dag=dag
    )
    
    final_task = DummyOperator(task_id="finaltask")
    
    # Flow.png'deki akışa göre task bağlantılarını ayarla
    dag_start >> [create_sample_data_task, insert_logs_task]
    create_sample_data_task >> copy_anomalies_task >> final_task
    insert_logs_task >> final_task 