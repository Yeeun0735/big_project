%spark

val df = spark.read.options(Map("sep"->",", "header"->"true")).
        csv("hdfs:///user/maria_dev/tmp/Cosmetic.csv")

df.createOrReplaceTempView("cosmetic")
        
df.printSchema()
df.show()


%sql
SELECT brand, count(brand)as cnt
FROM cosmetic GROUP BY brand ORDER BY cnt DESC
LIMIT 10


%sql
SELECT brand, count(brand)as cnt
FROM cosmetic WHERE category='스킨/토너' GROUP BY brand ORDER BY cnt DESC
LIMIT 10

%sql
SELECT brand, count(brand)as cnt
FROM cosmetic WHERE category='크림' GROUP BY brand ORDER BY cnt DESC
LIMIT 10

%sql
SELECT explode(split(t_rec, ',')) 
FROM tmp LIMIT 10 

%sql
SELECT recommand, count(recommand)
FROM cosmetic JOIN (
SELECT product, explode(split(rec_type,','))as recommand
FROM cosmetic)c ON cosmetic.product = c.product
where category='크림'
GROUP BY recommand
LIMIT 5

%sql
SELECT recommand, count(recommand)
FROM cosmetic JOIN (
SELECT product, explode(split(rec_type,','))as recommand
FROM cosmetic)c ON cosmetic.product = c.product
GROUP BY recommand
LIMIT 10

%sql
SELECT c.product, category, cosmetic.rating
FROM cosmetic JOIN (
SELECT product, explode(split(rec_type,','))as recommand
FROM cosmetic)c ON cosmetic.product = c.product
WHERE recommand = '트러블케어'and rating = 5
GROUP BY c.product, category, cosmetic.rating
order by rating
limit 20

%spark
val data = spark.read.options(Map("sep"->",", "header"->"true")).
        csv("hdfs:///user/maria_dev/tmp/Clothes.csv")
       
data.createOrReplaceTempView("clothes")
        
data.printSchema()
data.show()


%sql
SELECT category, count(category)as cnt
FROM clothes
GROUP BY category
order by cnt desc
LIMIT 5

%sql
SELECT category, count(category)as cnt
FROM clothes
GROUP BY category
order by cnt desc
LIMIT 5