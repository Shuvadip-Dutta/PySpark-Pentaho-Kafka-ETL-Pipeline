from pyspark.sql import SparkSession
from config import POSTGRES
from pyspark.sql.functions import (
    col,
    trim,
    to_date,
    coalesce,
    lit,
    from_json,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

# ===========================
# Spark Session
# ===========================

spark = (
    SparkSession.builder
    .appName("PySpark Kafka ETL")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ===========================
# PostgreSQL Configuration
# ===========================

TARGET_TABLE = "furniture_sales"

# ===========================
# Extract from Kafka
# ===========================

def extract():

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "furniture_sales")
        .option("startingOffsets", "earliest")
        .load()
    )

    schema = StructType([
        StructField("Salesperson", StringType()),
        StructField("Product", StringType()),
        StructField("Region", StringType()),
        StructField("Date", StringType()),
        StructField(" Item unit price ", DoubleType()),
        StructField(" No.Items ", IntegerType()),
        StructField(" Total Sale ", DoubleType())
    ])

    stream_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    return stream_df


# ===========================
# Transform
# ===========================

def transform(df):

    renamed = (
        df.withColumnRenamed("Salesperson", "salesperson")
        .withColumnRenamed("Product", "product")
        .withColumnRenamed("Region", "region")
        .withColumnRenamed("Date", "sale_date")
        .withColumnRenamed(" Item unit price ", "unit_price")
        .withColumnRenamed(" No.Items ", "num_items")
        .withColumnRenamed(" Total Sale ", "total_sale")
    )

    cleaned = (
        renamed

        # Clean salesperson
        .withColumn("salesperson", trim(col("salesperson")))

        # Product
        .withColumn(
            "product",
            when(
                trim(col("product")).isin(
                    "NaN", '"NaN"', "", "null", "NULL"
                ),
                "Unknown"
            ).otherwise(
                coalesce(trim(col("product")), lit("Unknown"))
            )
        )

        # Region
        .withColumn(
            "region",
            when(
                trim(col("region")).isin(
                    "NaN", '"NaN"', "", "null", "NULL"
                ),
                "Unknown"
            ).otherwise(
                coalesce(trim(col("region")), lit("Unknown"))
            )
        )

        # Clean date string first
        .withColumn(
            "sale_date",
            when(
                trim(col("sale_date")).isin(
                    "NaN", '"NaN"', "", "null", "NULL"
                ),
                None
            ).otherwise(trim(col("sale_date")))
        )

        # Convert only valid dates
        .withColumn(
            "sale_date",
            when(
                col("sale_date").rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),
                to_date(col("sale_date"), "M/d/yyyy")
            ).otherwise(None)
        )

        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("num_items", col("num_items").cast(IntegerType()))
        .withColumn("total_sale", col("total_sale").cast(DoubleType()))
    )

    # Remove rows having invalid date
    cleaned = cleaned.filter(col("sale_date").isNotNull())

    return cleaned.select(
        "salesperson",
        "product",
        "region",
        "sale_date",
        "unit_price",
        "num_items",
        "total_sale",
    )


# ===========================
# Load to PostgreSQL
# ===========================

def write_to_db(batch_df, batch_id):

    print(f"\n========== Batch {batch_id} ==========")

    batch_df.show(truncate=False)

    (
        batch_df.write
        .mode("append")
        .jdbc(
            url=POSTGRES["url"],
            table=TARGET_TABLE,
            properties={
                "user": POSTGRES["user"],
                "password": POSTGRES["password"],
                "driver": POSTGRES["driver"],
            },
        )
    )

    print(f"Batch {batch_id} completed.")


# ===========================
# Main
# ===========================

def main():

    raw_df = extract()

    clean_df = transform(raw_df)

    query = (
        clean_df.writeStream
        .foreachBatch(write_to_db)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()