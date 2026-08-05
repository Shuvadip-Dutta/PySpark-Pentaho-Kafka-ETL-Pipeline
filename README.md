# 🚀 PySpark Kafka ETL Pipeline

An end-to-end **Real-Time Data Engineering Pipeline** built using **Apache Kafka**, **Apache Spark Structured Streaming**, **PostgreSQL**, and **Docker**.

The pipeline continuously ingests sales data from a CSV file, streams it through Kafka, processes and cleans the data using PySpark, and stores the transformed data into PostgreSQL.

---

# 📌 Architecture

```
FurnitureSales.csv
        │
        ▼
Kafka Producer (Python)
        │
        ▼
Apache Kafka Topic
        │
        ▼
PySpark Structured Streaming
        │
        ▼
Data Cleaning & Transformation
        │
        ▼
PostgreSQL
        │
        ▼
pgAdmin
```

---

# 🛠 Tech Stack

- Python 3
- Apache Spark 4.x
- PySpark Structured Streaming
- Apache Kafka
- ZooKeeper
- PostgreSQL
- Docker & Docker Compose
- Pandas

---

# 📂 Project Structure

```
PySpark-Kafka-ETL
│
├── consumer_etl.py
├── producer.py
├── config.py
├── docker-compose.yml
├── requirements.txt
│
├── data/
│   └── FurnitureSales.csv
│
└── sql/
    └── create_table.sql
```

---

# ⚙️ Features

- Real-time data ingestion using Kafka
- Streaming ETL with PySpark
- Automatic schema parsing from Kafka JSON messages
- Data cleaning and transformation
- Missing value handling
- Invalid date handling
- PostgreSQL integration
- Dockerized environment

---

# 📊 Dataset

The pipeline processes furniture sales transactions containing:

| Column |
|---------|
| Salesperson |
| Product |
| Region |
| Date |
| Item Unit Price |
| No.Items |
| Total Sale |

---

# 🔄 Data Transformations

The Spark consumer performs the following transformations:

### Rename Columns

```
Salesperson → salesperson
Product → product
Region → region
Date → sale_date
Item unit price → unit_price
No.Items → num_items
Total Sale → total_sale
```

---

### Trim Spaces

Leading and trailing spaces are removed from text columns.

---

### Missing Value Handling

Missing products are replaced with:

```
Unknown
```

---

### Invalid Date Handling

Invalid dates such as

```
NaN
"NaN"
NULL
null
```

are safely converted to

```
NULL
```

using Spark's `try_to_date()`.

---

### Type Casting

```
unit_price → Double
total_sale → Double
num_items → Integer
sale_date → Date
```

---

# 🐳 Docker Services

The project runs four containers:

| Container | Purpose |
|------------|----------|
| Kafka | Message Broker |
| ZooKeeper | Kafka Coordination |
| Spark | ETL Processing |
| PostgreSQL | Data Storage |

---

# 🚀 How to Run

## 📦 Installation

Clone the repository:

```bash
git clone https://github.com/Shuvadip-Dutta/PySpark-Kafka-ETL.git
cd PySpark-Kafka-ETL
```

Create a virtual environment (optional but recommended):

```bash
python -m venv venv
```

Activate the virtual environment

### Windows

```bash
venv\Scripts\activate
```

### Linux / macOS

```bash
source venv/bin/activate
```

Install the required Python packages:

```bash
pip install -r requirements.txt
```

## 🐘 PostgreSQL Configuration

This project uses PostgreSQL as the destination database for the ETL pipeline.

### Option 1: Using Docker (Recommended)

The Docker Compose file automatically starts a PostgreSQL container.

Default configuration:

```text
Host: localhost
Port: 5432 (or 5433 if changed in docker-compose.yml)
Database: pyspark
Username: postgres
Password: 834585
```

Connect using pgAdmin or any PostgreSQL client with the above credentials.

---

### Option 2: Local PostgreSQL Installation

If you are using a locally installed PostgreSQL server, update the database configuration in `config.py`:

```python
POSTGRES = {
    "url": "jdbc:postgresql://localhost:5432/pyspark",
    "driver": "org.postgresql.Driver",
    "user": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD",
}
```

Also update the following inside `consumer_etl.py` if needed:

```python
DB_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/pyspark",
    "driver": "org.postgresql.Driver",
    "user": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD",
}
```

## 🖥️ Connect PostgreSQL with pgAdmin

If you're using **Docker PostgreSQL**, you need to create a new server in pgAdmin.

1. Open **pgAdmin**.
2. Right-click **Servers** → **Register** → **Server**.

### General

```text
Name: PySpark PostgreSQL
```

### Connection 

```text
Host name/address: localhost
Port: 5433
Maintenance database: postgres
Username: postgres
Password: <your_password>
```

> **Note:** Replace `<your_password>` with the password configured in your `docker-compose.yml`.

Click **Save**.

after running the 
```bash
docker compose up -d
```
and after creating the sql tabel

You should see:

```
Servers
└── PySpark PostgreSQL
    └── Databases
        └── pyspark
            └── Schemas
                └── public
                    └── Tables
                        └── furniture_sales
```

If the table is not visible:

- Right-click **Tables**
- Click **Refresh**

You can verify the imported data by opening the **Query Tool** and running:

```sql
SELECT * FROM furniture_sales;
```

or

```sql
SELECT COUNT(*) FROM furniture_sales;
```

---

## Step 1

Start Docker containers

```bash
docker compose up -d
```

---

## Step 2

Create PostgreSQL table

```bash
docker exec -it postgres psql -U postgres -d pyspark
```

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

## Step 3

Create Kafka Topic

```bash
docker exec -it kafka kafka-topics \
--bootstrap-server kafka:29092 \
--create \
--topic furniture_sales \
--partitions 1 \
--replication-factor 1
```

---

## Step 4

Start Spark Streaming

```bash
docker exec -it spark bash
```

Inside container

```bash
cd /app
```

Run

```bash
/opt/spark/bin/spark-submit \
--conf spark.jars.ivy=/tmp/.ivy2 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,org.postgresql:42.7.3 \
/app/consumer_etl.py
```

---

## Step 5

Run Producer

```bash
python producer.py
```

Producer reads the CSV and streams records into Kafka.

---

## Step 6

Verify Data

```bash
docker exec -it postgres psql -U postgres -d pyspark
```

```sql
SELECT * FROM furniture_sales;
```

---

# 📈 Example Output

```
Salesperson 1
Chair
NW
2019-03-13
340.95
6
2045.70
```

---

# 📦 Kafka Topic

```
furniture_sales
```

---

# 🗄 PostgreSQL Table

```
furniture_sales
```

---

# 🧹 Data Quality Improvements

✔ Removed extra spaces

✔ Renamed columns

✔ Safe date parsing

✔ Null handling

✔ Missing product replacement

✔ Proper datatype conversion

---

# 📸 Screenshots

You can add screenshots here:

- Docker Containers Running
- Kafka Topic
- Spark Streaming Logs
- PostgreSQL Table
- pgAdmin
- Project Architecture

---

# 👨‍💻 Author

**Shuvadip Dutta**

- GitHub: https://github.com/Shuvadip-Dutta
- LinkedIn: https://www.linkedin.com/in/shuvadip-dutta/

---

# ⭐ If you found this project useful, consider giving it a star!
