# Pentaho + Kafka + PostgreSQL ETL Pipeline

This README documents the Pentaho version of the Furniture Sales Streaming ETL pipeline.

## Workflow

```text
Python Producer
      |
      v
Kafka (furniture_sales)
      |
      v
Pentaho Parent: Kafka Consumer
      |
      v
Pentaho Child Transformation
      |
      +--> Get records from stream
      +--> JSON input
      +--> Select Fields
      +--> Filter Rows
      +--> Replace in String
      +--> If Field Value Is Null
      +--> Unique Rows
      +--> Select Values (Date conversion)
      +--> Table Output
                  |
                  v
             PostgreSQL
                  |
                  v
                pgAdmin
```

---

## 1. Start Docker

From the project directory:

```cmd
docker compose up -d
docker ps
```

For the Pentaho implementation, Spark is not required in the execution path. If a Spark Docker service is running and is not needed:

```cmd
docker compose stop spark
```

If the container has a different name:

```cmd
docker stop <spark-container-name>
```

Do not delete the Spark code; it can remain as the alternative PySpark implementation.

---

## 2. Kafka Topic

List topics:

```cmd
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Create the topic if it does not exist:

```cmd
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create --topic furniture_sales --partitions 1 --replication-factor 1
```

Check again:

```cmd
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list
```

### Kafka ports

Pentaho is running on Windows, so use:

```text
localhost:9092
```

Docker Kafka CLI uses the Docker-network address:

```text
kafka:29092
```

Therefore:

```text
Pentaho -> localhost:9092
Docker Kafka CLI -> kafka:29092
```

---

## 3. Check Kafka Streaming

Open another terminal:

```cmd
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic furniture_sales --from-beginning
```

When `producer.py` sends data, JSON messages should appear.

Example:

```json
{
  "Salesperson": "Salesperson 1",
  "Product": "Chair",
  "Region": "NW",
  "Date": "3/13/2019",
  "Item unit price": 340.95,
  "No.Items": 6,
  "Total Sale": 2045.7
}
```

---

## 4. Delete and Recreate Kafka Topic

Delete:

```cmd
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic furniture_sales
```

Verify:

```cmd
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Recreate:

```cmd
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create --topic furniture_sales --partitions 1 --replication-factor 1
```

> Deleting the topic removes its stored Kafka messages.

---

## 5. Create PostgreSQL Table Inside Docker

Open PostgreSQL:

```cmd
docker exec -it postgres psql -U postgres -d pyspark
```

Create the table:

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

Check it:

```sql
\d furniture_sales
SELECT COUNT(*) FROM furniture_sales;
```

Exit:

```sql
\q
```

---

## 6. PostgreSQL Setup in pgAdmin

Create/add a PostgreSQL server in pgAdmin.

Typical Docker PostgreSQL settings:

```text
Host name/address: localhost
Port: 5432
Maintenance database: postgres
Username: postgres
Password: <your PostgreSQL password>
```

Open:

```text
Databases
  -> pyspark
     -> Schemas
        -> public
           -> Tables
              -> furniture_sales
```

Refresh the Tables node if required.

### If port 5432 is occupied

If Docker maps PostgreSQL as:

```text
5433:5432
```

then pgAdmin uses:

```text
Host: localhost
Port: 5433
Database: pyspark
Username: postgres
Password: <your PostgreSQL password>
```

Use the port actually configured in Docker.

---

## 7. Pentaho PostgreSQL Connection

In Pentaho Data Integration / Spoon, create:

```text
Connection name: PostgreSQL_PySpark
Database type: PostgreSQL
Host: localhost
Port: 5432
Database: pyspark
Username: postgres
Password: <your PostgreSQL password>
```

If Docker exposes PostgreSQL on 5433, use 5433 instead.

Test the connection.

---

## 8. Pentaho Kafka Consumer

In the parent transformation's Kafka Consumer step:

```text
Connection: Direct
Bootstrap servers: localhost:9092
Topic: furniture_sales
Consumer group: pentaho-furniture-group
```

Do not normally use `kafka:29092` here because Pentaho is running on Windows.

---

## 9. Parent and Child Transformations

The parent transformation contains the Kafka Consumer.

The parent passes streaming messages into the child transformation.

The parent should reference the saved child `.ktr` file in the Kafka Consumer step.

### Run only the parent

After the parent and child are configured and saved:

```text
Run Kafka_to_PostgreSQL
```

Do not manually run the child separately for the normal streaming workflow.

The parent starts the child transformation.

---

## 10. Child Transformation

The child flow is:

```text
Get records from stream
        |
        v
JSON input
        |
        v
Select Fields
        |
        v
Filter Rows
        |
        v
Replace in String
        |
        v
If Field Value Is Null
        |
        v
Unique Rows
        |
        v
Select Values
        |
        v
Table Output
```

### Get records from stream

Receive the Kafka JSON message from the parent.

The important field is:

```text
Message
```

### JSON input

Configure:

```text
Source from field: Message
```

Map the JSON data to the same logical fields used by the PostgreSQL table:

| Field | JSONPath | Type |
|---|---|---|
| salesperson | `$.Salesperson` | String |
| product | `$.Product` | String |
| region | `$.Region` | String |
| sale_date | `$.Date` | String |
| unit_price | `$. Item unit price ` | Number |
| num_items | `$. No.Items ` | Integer |
| total_sale | `$. Total Sale ` | Number |

Use the exact JSON keys/paths produced by `producer.py`.

**Keep `sale_date` as String at this stage.** Some source records contain a NULL date.

---

## 11. Select Fields

Keep only the fields required by PostgreSQL:

```text
salesperson
product
region
sale_date
unit_price
num_items
total_sale
```

Remove unnecessary Kafka metadata such as:

```text
Key
Message
Topic
Partition
Offset
Timestamp
```

---

## 12. Filter Rows

Filter invalid records before Date conversion.

Recommended condition:

```text
salesperson IS NOT NULL
AND
sale_date IS NOT NULL
```

Send:

```text
TRUE  -> Replace in String
FALSE -> discard/no next step
```

This prevents invalid NULL salesperson/date values from reaching later processing.

---

## 13. Replace in String

Use this step to clean unwanted/special characters from string fields where required.

Typical fields:

```text
salesperson
product
region
```

Do not use this step as the main String -> Date conversion.

---

## 14. If Field Value Is Null

Use this step for string fields that should remain in the data.

For example:

```text
product -> Unknown
region  -> Unknown
```

So:

```text
product = NULL
```

can become:

```text
product = Unknown
```

Do not convert a NULL date into a date. Missing dates should already have been removed by Filter Rows.

---

## 15. Unique Rows

Use **Unique Rows** to remove duplicate records before inserting into PostgreSQL.

This is useful when the Kafka topic contains repeated messages or when the same record is processed more than once.

---

## 16. Select Values - Date Conversion

After filtering and cleaning, convert:

```text
sale_date
String -> Date
```

Use source format:

```text
MM/dd/yyyy
```

Example:

```text
3/13/2019 -> 2019-03-13
```

This matches:

```sql
sale_date DATE
```

Do this conversion after NULL filtering.

---

## 17. Table Output

Configure:

```text
Connection: PostgreSQL_PySpark
Target schema: public
Target table: furniture_sales
```

Map:

| Table field | Stream field |
|---|---|
| salesperson | salesperson |
| product | product |
| region | region |
| sale_date | sale_date |
| unit_price | unit_price |
| num_items | num_items |
| total_sale | total_sale |

---

## 18. Run the Complete Pipeline

### Terminal 1 - Docker

```cmd
docker compose up -d
docker ps
```

### Pentaho - Start the parent

Open and run:

```text
Kafka_to_PostgreSQL
```

Run the **parent only**.

### Terminal 2 - Python producer

From the project virtual environment:

```cmd
python producer.py
```

The producer sends records to:

```text
furniture_sales
```

Kafka streams the messages to Pentaho.

Pentaho cleans/transforms the data and inserts it into PostgreSQL.

### Optional Kafka verification

```cmd
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic furniture_sales --from-beginning
```

---

## 19. Check Pentaho Metrics

Verify records moving through:

```text
Kafka Consumer
      |
Get records from stream
      |
JSON input
      |
Select Fields
      |
Filter Rows
      |
Replace in String
      |
If Field Value Is Null
      |
Unique Rows
      |
Select Values
      |
Table Output
```

Check:

```text
Read
Written
Errors
```

The most important value is:

```text
Errors = 0
```

Counts may decrease after Filter Rows and Unique Rows. That is expected.

---

## 20. Check PostgreSQL

Open:

```cmd
docker exec -it postgres psql -U postgres -d pyspark
```

Run:

```sql
SELECT COUNT(*) FROM furniture_sales;
SELECT * FROM furniture_sales;
SELECT * FROM furniture_sales LIMIT 10;
```

Check for invalid salesperson/date values:

```sql
SELECT *
FROM furniture_sales
WHERE salesperson IS NULL
   OR sale_date IS NULL;
```

Check column types:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'furniture_sales';
```

Exit:

```sql
\q
```

---

## 21. Check pgAdmin

Go to:

```text
Servers
 -> PostgreSQL
    -> Databases
       -> pyspark
          -> Schemas
             -> public
                -> Tables
                   -> furniture_sales
```

Right-click the table and:

```text
Refresh
```

Then:

```text
View/Edit Data -> All Rows
```

---

# 22. Troubleshooting

## Kafka topic not found

```cmd
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Create it:

```cmd
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create --topic furniture_sales --partitions 1 --replication-factor 1
```

---

## Pentaho cannot connect to Kafka

For Pentaho on Windows:

```text
localhost:9092
```

For Kafka CLI inside Docker:

```text
kafka:29092
```

---

## Kafka has data but Pentaho gets nothing

Check:

```cmd
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic furniture_sales --from-beginning
```

If messages appear:

1. Check Pentaho bootstrap server.
2. Check topic name.
3. Check consumer group.
4. Make sure the parent is running.
5. Make sure the Kafka Consumer references the saved child `.ktr`.

---

## "The transformation path is missing"

Open the Kafka Consumer step.

Use **Browse** and select the saved child transformation `.ktr`.

The Transformation field must not be empty.

---

## JSON input reads 0

Make sure:

```text
Source from field = Message
```

Check that the parent is passing the `message` field.

Also verify the JSONPath against the actual Kafka JSON.

---

## JSONPath error

Check paths such as:

```text
$.Salesperson
$.Product
$.Region
$.Date
```

For keys containing spaces or unusual characters, use the exact JSON key/path supported by the Pentaho JSON Input step.

---

## Data stops at Filter Rows

Start with the simple condition:

```text
sale_date IS NOT NULL
```

Then:

```text
TRUE -> Replace in String
FALSE -> discard
```

If Replace in String reads 0, the filter condition is excluding all records.

After that works, add:

```text
salesperson IS NOT NULL
```

---

## Date conversion error

Example:

```text
couldn't convert string [NULL] to a date
```

Keep:

```text
sale_date = String
```

in JSON Input.

Then:

```text
JSON input
 -> Filter Rows
 -> cleaning
 -> Select Values
 -> sale_date String -> Date
```

Use:

```text
MM/dd/yyyy
```

---

## PostgreSQL says sale_date is VARCHAR

The Pentaho stream is still String.

In:

```text
Select Values -> Meta-data
```

change:

```text
sale_date: String
```

to:

```text
sale_date: Date
```

with:

```text
MM/dd/yyyy
```

---

## PostgreSQL table does not exist

Open:

```cmd
docker exec -it postgres psql -U postgres -d pyspark
```

Check:

```sql
\dt
```

Create the table using the SQL in Section 5 if necessary.

---

## pgAdmin does not show new records

First verify:

```sql
SELECT COUNT(*) FROM furniture_sales;
```

If the count is correct, refresh the table in pgAdmin.

Also verify that pgAdmin is connected to the same PostgreSQL instance as Pentaho.

---

## Duplicate rows appear

Kafka retains messages in the topic.

Check:

```cmd
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic furniture_sales --from-beginning
```

For a clean test, delete and recreate the topic:

```cmd
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic furniture_sales
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create --topic furniture_sales --partitions 1 --replication-factor 1
```

You can also clear the destination table:

```sql
TRUNCATE TABLE furniture_sales;
```

---

## Two PostgreSQL instances / port conflict

Check:

```cmd
netstat -ano | findstr :5432
```

Check Docker:

```cmd
docker ps
```

Make sure Pentaho and pgAdmin are connected to the same PostgreSQL instance.

---

# 23. Final ETL Logic

The child transformation performs:

1. Receive streaming data from the parent Kafka Consumer.
2. Parse the Kafka JSON message.
3. Select required fields and remove unnecessary Kafka metadata.
4. Filter rows where `salesperson` or `sale_date` is NULL.
5. Remove/replace unwanted special characters.
6. Replace NULL `product` and `region` values with `Unknown`.
7. Remove duplicate records.
8. Convert `sale_date` from String to Date using `MM/dd/yyyy`.
9. Insert the cleaned records into PostgreSQL.
10. Verify the final data in pgAdmin.

Final architecture:

```text
Python producer.py
        |
        v
Kafka furniture_sales
        |
        v
Pentaho Parent
Kafka Consumer
        |
        v
Pentaho Child
JSON -> Select -> Filter -> Clean -> Null handling
     -> Unique -> Date conversion -> Table Output
        |
        v
PostgreSQL pyspark.furniture_sales
        |
        v
pgAdmin
```
