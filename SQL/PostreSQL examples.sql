CREATE TABLE kullanicilar (
   kullanici_id integer,
   isim varchar,
   sehir varchar
);

CREATE TABLE siparisler (
   siparis_id integer,
   kullanici_id integer,
   urun_adi varchar,
   fiyat decimal
);

INSERT INTO kullanicilar (kullanici_id, isim, sehir) VALUES 
   (1, 'Ahmet', 'İstanbul'),
   (2, 'Ayşe', 'Ankara'),
   (3, 'Fatma', 'İzmir'),
   (4, 'Mehmet', 'İstanbul');
  
INSERT INTO siparisler (siparis_id, kullanici_id, urun_adi, fiyat) VALUES
   (1, 1, 'Telefon', 1000),
   (2, 2, 'Bilgisayar', 2000),
   (3, 3, 'Tablet', 500),
   (4, 1, 'Kulaklık', 200),
   (5, 2, 'Yazıcı', 1500),
   (6, 4, 'Kılıf', 100);
   

--projection

select * from kullanicilar k
join siparisler s on k.kullanici_id = s.kullanici_id;


INSERT INTO kullanicilar (kullanici_id, isim, sehir) VALUES
  (5, 'Gökhan', 'Çanakkale');
  
INSERT INTO siparisler (siparis_id, kullanici_id, urun_adi, fiyat) VALUES
  (7, 6, 'Kalem', 1000);

--left join
select * from kullanicilar k left join siparisler s on k.kullanici_id = s.kullanici_id;

--right join
select * from kullanicilar k right join siparisler s on k.kullanici_id = s.kullanici_id;


select * from kullanicilar k
inner join siparisler s on k.kullanici_id = s.kullanici_id
where k.isim = 'Ahmet';


select * from kullanicilar k
inner join siparisler s on k.kullanici_id = s.kullanici_id
where k.isim like 'A%';


select k.kullanici_id,k.isim,urun_adi,fiyat from kullanicilar k
inner join siparisler s on k.kullanici_id = s.kullanici_id
where k.isim like 'A%';

-- İsmi A ile başlayan müşterilerin müşteri bazında toplam sipariş fiyatı ne kadardır ? 
-- çıktı şu şekilde olmalıdır : isim/toplamfiyat

--aggregation işlemlerinde aggregation işlemi yaptığımız kolon hariç bütün kolonlar group by işlemine tabii tutulur.
select k.isim, 
sum(s.fiyat) as toplamfiyat 
from kullanicilar k 
inner join siparisler s on k.kullanici_id = s.kullanici_id
where k.isim like 'A%'
group by k.kullanici_id,k.isim;



select k.isim,
k.kullanici_id,
sum(s.fiyat) as toplamfiyat,
avg(s.fiyat) as ortalamafiyat,
min(s.fiyat) as minfiyat,
max(s.fiyat) as maxfiyat
from kullanicilar k 
inner join siparisler s on k.kullanici_id = s.kullanici_id
group by k.kullanici_id,k.isim;


select k.isim,
k.kullanici_id,
sum(s.fiyat) as toplamfiyat,
avg(s.fiyat) as ortalamafiyat,
min(s.fiyat) as minfiyat,
max(s.fiyat) as maxfiyat,
count(s.siparis_id) as siparissayi
from kullanicilar k 
inner join siparisler s on k.kullanici_id = s.kullanici_id
group by k.kullanici_id,k.isim
having count(s.siparis_id) > 1;



CREATE VIEW  ozet_data
AS 
select k.isim,
k.kullanici_id,
sum(s.fiyat) as toplamfiyat,
avg(s.fiyat) as ortalamafiyat,
min(s.fiyat) as minfiyat,
max(s.fiyat) as maxfiyat,
count(s.siparis_id) as siparissayi
from kullanicilar k 
inner join siparisler s on k.kullanici_id = s.kullanici_id
group by k.kullanici_id,k.isim;


--select * from ozet_data


select k.isim,
k.kullanici_id, 
sum(s.fiyat) as toplamfiyat,
avg(s.fiyat) as ortalamafiyat,
min(s.fiyat) as minfiyat,
max(s.fiyat) as maxfiyat,
count(s.siparis_id) as siparissayi
from kullanicilar k 
inner join siparisler s on k.kullanici_id = s.kullanici_id
group by k.kullanici_id,k.isim;


with ozetdata_with as (
select
k.kullanici_id,
k.isim,
sum(s.fiyat) as toplamfiyat,
avg(s.fiyat) as ortalamafiyat,
min(s.fiyat) as minfiyat,
max(s.fiyat) as maxfiyat,
count(s.siparis_id) as siparissayi
from kullanicilar k 
inner join siparisler s on k.kullanici_id = s.kullanici_id
group by k.kullanici_id,k.isim
),
tmp as(
select 1 as a, 'deneme' as b
)

select * from ozetdata_with o
inner join tmp t on o.kullanici_id = t.a

--inner join ve left join sektördeki en önemli sql işlemlerindendir.


