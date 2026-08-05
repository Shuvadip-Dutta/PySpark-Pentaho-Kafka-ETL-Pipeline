# 🛠 Troubleshooting Guide

## 1. Kafka Producer Error

### Error

```text
KafkaTimeoutError: Failed to update metadata after 60.0 secs
```

### Cause

Producer cannot connect to Kafka.

### Solution

Check Kafka is running.

```bash
docker ps
```

Verify topic exists.

```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Verify `producer.py` uses

```python
bootstrap_servers="localhost:9092"
```

Verify Spark uses

```python
.option("kafka.bootstrap.servers","kafka:29092")
```

---

## 2. Kafka Topic Does Not Exist

### Error

```text
UnknownTopicOrPartitionException
```

### Solution

Create the topic.

```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create --topic furniture_sales --partitions 1 --replication-factor 1
```

Verify

```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list
```

---

## 3. spark-submit Not Found

### Error

```text
spark-submit: command not found
```

### Solution

Use the full path.

```bash
/opt/spark/bin/spark-submit \
--conf spark.jars.ivy=/tmp/.ivy2 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,org.postgresql:postgresql:42.7.3 \
/app/consumer_etl.py
```

---

## 4. CSV File Not Found

### Error

```text
FileNotFoundError:
```

### Cause

Wrong CSV path.

### Solution

Use

```python
pd.read_csv("data/FurnitureSales.csv")
```

instead of

```python
pd.read_csv("../data/FurnitureSales.csv")
```

---

## 5. PostgreSQL Table Does Not Exist

### Error

```text
relation "furniture_sales" does not exist
```

### Solution

Create the table.

```sql
CREATE TABLE furniture_sales (
    salesperson VARCHAR(100),
    product VARCHAR(100),
    region VARCHAR(50),
    sale_date DATE,
    unit_price DOUBLE PRECISION,
    num_items INTEGER,
    total_sale DOUBLE PRECISION
);
```

---

## 6. Spark Date Parsing Error

### Error

```text
CANNOT_PARSE_TIMESTAMP
Text '"NaN"' could not be parsed
```

### Cause

Dataset contains invalid dates.

### Solution

Use

```python
expr("try_to_date(sale_date,'M/d/yyyy')")
```

instead of

```python
to_date()
```

---

## 7. Product Appears as "NaN"

### Cause

Dataset contains missing product values.

### Solution

Replace with

```python
Unknown
```

using

```python
when(
    trim(col("product")).isin("NaN", '"NaN"', ""),
    "Unknown"
)
```

---

## 8. Spark Cannot Connect to Kafka

### Cause

Wrong Kafka listener.

### Solution

Inside Docker use

```python
kafka:29092
```

Outside Docker use

```python
localhost:9092
```

---

## 9. Producer Cannot Send Messages

### Check

List Kafka topic

```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Check producer configuration

```python
bootstrap_servers="localhost:9092"
```

---

## 10. Data Not Visible in pgAdmin

### Cause

Connected to Windows PostgreSQL instead of Docker PostgreSQL.

### Verify Docker PostgreSQL

```bash
docker exec -it postgres psql -U postgres -d pyspark
```

```sql
SELECT version();
```

Should show

```
PostgreSQL 16 (Debian)
```

If pgAdmin shows

```
PostgreSQL 17 (Windows)
```

you're connected to the wrong server.

Connect using

```
Host: localhost
Port: 5433
Database: pyspark
Username: postgres
Password: <your_password>
```

---

## 11. Kafka Topic Missing After Restart

### Cause

Containers recreated.

### Solution

Recreate topic.

```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create --topic furniture_sales --partitions 1 --replication-factor 1
```

---

## 12. Verify Data in PostgreSQL

```bash
docker exec -it postgres psql -U postgres -d pyspark
```

```sql
SELECT COUNT(*) FROM furniture_sales;
```

```sql
SELECT * FROM furniture_sales LIMIT 10;
```

---

## 13. Clean Docker Environment

Stop everything

```bash
docker compose down
```

Remove volumes

```bash
docker compose down -v
```

Remove unused resources

```bash
docker system prune -f
```

---

## 14. Verify Running Containers

```bash
docker ps
```

Expected

```
postgres
zookeeper
kafka
spark
```

---

## 15. Complete Execution Order

```text
docker compose up -d

↓

Create PostgreSQL table

↓

Create Kafka topic

↓

Run Spark Consumer

↓

Run producer.py

↓

Verify PostgreSQL
```