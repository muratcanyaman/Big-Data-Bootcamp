# Airflow Data Pipeline Projesi

## Proje Hakkında
Bu proje, Apache Airflow kullanarak veri işleme ve analiz görevlerini otomatikleştiren bir veri hattı (data pipeline) uygulamasıdır. Proje kapsamında, düzenli aralıklarla rastgele sıcaklık ve nem verileri oluşturularak MongoDB'ye kaydedilmekte, bu verilerden anormal değerler (30°C üzeri sıcaklıklar) tespit edilerek farklı bir koleksiyona kopyalanmakta ve Airflow'un kendi log kayıtları analiz edilerek MongoDB'de saklanmaktadır.

## Proje İçeriği
Projede, Flow.png dosyasında tasarlanan iş akışı Airflow DAG'ları kullanılarak uygulanmıştır. DAG her 5 dakikada bir çalışarak veri oluşturma, anormallik tespiti ve log analizi işlemlerini gerçekleştirir.

## Kullanılan Teknolojiler
- **Apache Airflow**: İş akışı otomasyonu ve zamanlanması
- **MongoDB**: Veri depolama ve yönetimi
- **PostgreSQL**: Airflow'un dahili veri tabanı
- **Docker & Docker Compose**: Containerization ve orchestration
- **Python**: Veri işleme ve analiz

## Mimari Yapı
Proje, üç ana bileşenden oluşmaktadır:
1. **Veri Üretimi**: Rastgele sıcaklık ve nem verileri oluşturulur ve MongoDB'ye kaydedilir.
2. **Anomali Tespiti**: 30°C'den yüksek sıcaklık değerleri anomali olarak tespit edilip ayrı bir koleksiyona kaydedilir.
3. **Log Analizi**: Airflow'un log kayıtları incelenerek event bazlı analiz yapılır ve sonuçlar MongoDB'ye kaydedilir.

## Kurulum ve Çalıştırma
Projeyi çalıştırmak için Docker ve Docker Compose kurulu olmalıdır.

### Çalıştırmak için

```bash
docker compose -f docker-compose.yaml build
docker compose -f docker-compose.yaml up
```

### Durdurmak için

```bash
docker compose -f docker-compose.yaml down
```

## Uygulama Detayları
Uygulama, temel olarak iki DAG dosyasından oluşur:
- **demo.py**: Airflow'un temel kullanımını gösteren basit bir örnek.
- **homework.py**: Ana proje iş akışını içeren DAG.

Uygulama çalıştığında Airflow web arayüzüne tarayıcınızdan `http://localhost:8080` adresinden erişebilirsiniz. Kullanıcı adı ve şifre varsayılan olarak "airflow" olarak ayarlanmıştır.

## Veri Akışı
1. Her 5 dakikada bir, rastgele 10 adet sıcaklık ve nem verisi oluşturulur.
2. Bu veriler MongoDB'deki ilgili koleksiyona kaydedilir.
3. Sıcaklığı 30°C'den yüksek olan kayıtlar tespit edilir ve ayrı bir koleksiyona kopyalanır.
4. Paralel olarak, Airflow'un son 1 dakikadaki log kayıtları analiz edilir ve sonuçlar MongoDB'ye kaydedilir.


## Proje Yapısı

```text
airflow/
├── Flow.png                 # İş akışı diyagramı
├── README.md                # Proje dokümantasyonu
├── docker-compose.yaml      # Docker Compose yapılandırması
├── Dockerfile               # Docker container yapılandırması
├── requirements.txt         # Python bağımlılıkları
└── src/                     # Kaynak dosyaları
    ├── dags/                # Airflow DAG dosyaları
    │   ├── demo.py          # Örnek DAG
    │   ├── homework.py      # Ana proje DAG'ı
    │   └── models/          # Veri modelleri
    │       └── heat_and_humidity.py  # Sıcaklık ve nem veri modeli
    ├── logs/                # Airflow log dosyaları
    ├── plugins/             # Airflow eklentileri
    └── etc/                 # Diğer yapılandırma dosyaları
```

Bu proje, modern veri mühendisliği pratiklerini uygulayarak, veri işleme ve analiz süreçlerinin otomatikleştirilmesine odaklanmaktadır. Sensör verileri gibi IoT cihazlarından gelen verilerin toplanması, anormallik tespiti ve log analizi gibi gerçek dünya uygulamalarını simüle etmektedir.
