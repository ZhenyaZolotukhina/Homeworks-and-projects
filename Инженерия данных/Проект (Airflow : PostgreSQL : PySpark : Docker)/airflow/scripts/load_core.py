import os
import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

DATA_PATH = "/opt/airflow/data/team1"

PG_HOST = "postgres"
PG_PORT = "5432"
PG_DB = "deliveries"
PG_USER = "postgres"
PG_PASSWORD = "postgres"

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}
POSTGRES_JAR = "/opt/airflow/postgresql.jar"


def truncate_tables():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            """
            TRUNCATE TABLE
              order_items,
              driver_assignment,
              orders,
              users,
              drivers,
              stores,
              items
            RESTART IDENTITY CASCADE;
            """
        )
    conn.close()


def first_nn(colname: str):
    return F.first(F.col(colname), ignorenulls=True).alias(colname)


def main():
    spark = (
    SparkSession.builder.appName("load_core_tables")
    .master("local[2]")
    .config("spark.jars", POSTGRES_JAR)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    .getOrCreate()
)

    df = spark.read.option("recursiveFileLookup", "true").parquet(DATA_PATH).dropDuplicates()

    df = (
        df.withColumn("order_id", F.col("order_id").cast(T.LongType()))
        .withColumn("user_id", F.col("user_id").cast(T.LongType()))
        .withColumn("driver_id", F.col("driver_id").cast(T.LongType()))
        .withColumn("store_id", F.col("store_id").cast(T.LongType()))
        .withColumn("item_id", F.col("item_id").cast(T.LongType()))
        .withColumn("item_replaced_id", F.col("item_replaced_id").cast(T.LongType()))
    )

    df = df.withColumn(
        "order_item_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("order_id").cast("string"),
                F.col("item_id").cast("string"),
                F.col("item_quantity").cast("string"),
                F.col("item_price").cast("string"),
                F.col("item_discount").cast("string"),
                F.col("item_canceled_quantity").cast("string"),
                F.coalesce(F.col("item_replaced_id").cast("string"), F.lit("NULL")),
                F.col("driver_id").cast("string"),
                F.col("store_id").cast("string"),
                F.col("created_at").cast("string"),
            ),
            256,
        ),
    )

    users_df = df.select("user_id", "user_phone").dropDuplicates(["user_id"])
    drivers_df = df.select("driver_id", "driver_phone").dropDuplicates(["driver_id"])
    stores_df = df.select("store_id", "store_address").dropDuplicates(["store_id"])
    items_df = df.select("item_id", "item_title", "item_category").dropDuplicates(["item_id"])

    orders_df = (
        df.groupBy("order_id")
        .agg(
            first_nn("address_text"),
            first_nn("created_at"),
            first_nn("paid_at"),
            first_nn("delivery_started_at"),
            first_nn("delivered_at"),
            first_nn("canceled_at"),
            first_nn("payment_type"),
            first_nn("order_discount"),
            first_nn("order_cancellation_reason"),
            first_nn("delivery_cost"),
            first_nn("user_id"),
            first_nn("store_id"),
            F.first(
                F.when(F.col("delivered_at").isNotNull(), F.col("driver_id")),
                ignorenulls=True,
            ).alias("driver_id"),
        )
        .withColumn("order_id", F.col("order_id").cast(T.LongType()))
        .withColumn("user_id", F.col("user_id").cast(T.LongType()))
        .withColumn("store_id", F.col("store_id").cast(T.LongType()))
        .withColumn("driver_id", F.col("driver_id").cast(T.LongType()))
    )

    driver_assignment_df = (
        df.select("order_id", "driver_id", "delivered_at")
        .dropDuplicates(["order_id", "driver_id", "delivered_at"])
        .withColumn("order_id", F.col("order_id").cast(T.LongType()))
        .withColumn("driver_id", F.col("driver_id").cast(T.LongType()))
    )

    base_items = df.select(
        "order_item_id",
        "order_id",
        "item_id",
        "item_quantity",
        "item_price",
        "item_discount",
        "item_canceled_quantity",
        "item_replaced_id",
    )

    orig = (
        base_items.filter(F.col("item_replaced_id").isNotNull())
        .select(
            F.col("order_id").alias("o_order_id"),
            F.col("order_item_id").alias("orig_order_item_id"),
            F.col("item_replaced_id").alias("replacement_item_id"),
        )
    )

    repl_candidates = base_items.select(
        F.col("order_id").alias("r_order_id"),
        F.col("item_id").alias("r_item_id"),
        F.col("order_item_id").alias("repl_order_item_id"),
    )

    joined = orig.join(
        repl_candidates,
        (orig.o_order_id == repl_candidates.r_order_id)
        & (orig.replacement_item_id == repl_candidates.r_item_id),
        "left",
    )

    w = Window.partitionBy("orig_order_item_id").orderBy(F.col("repl_order_item_id").asc())

    map_repl_to_orig = (
        joined.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("repl_order_item_id", "orig_order_item_id")
        .filter(F.col("repl_order_item_id").isNotNull())
    )

    order_items_df = (
        base_items.drop("item_replaced_id")
        .join(
            map_repl_to_orig,
            on=base_items.order_item_id == map_repl_to_orig.repl_order_item_id,
            how="left",
        )
        .withColumnRenamed("orig_order_item_id", "replaces_order_item_id")
        .drop("repl_order_item_id")
        .select(
            "order_item_id",
            "order_id",
            "item_id",
            "item_quantity",
            "item_price",
            "item_discount",
            "item_canceled_quantity",
            "replaces_order_item_id",
        )
        .withColumn("order_id", F.col("order_id").cast(T.LongType()))
        .withColumn("item_id", F.col("item_id").cast(T.LongType()))
    )

    truncate_tables()

    users_df.write.jdbc(JDBC_URL, "users", mode="append", properties=JDBC_PROPS)
    drivers_df.write.jdbc(JDBC_URL, "drivers", mode="append", properties=JDBC_PROPS)
    stores_df.write.jdbc(JDBC_URL, "stores", mode="append", properties=JDBC_PROPS)
    items_df.write.jdbc(JDBC_URL, "items", mode="append", properties=JDBC_PROPS)
    orders_df.write.jdbc(JDBC_URL, "orders", mode="append", properties=JDBC_PROPS)
    driver_assignment_df.write.jdbc(JDBC_URL, "driver_assignment", mode="append", properties=JDBC_PROPS)
    order_items_df = order_items_df.dropDuplicates(["order_item_id"]).repartition(8)
    order_items_df.write.jdbc(JDBC_URL, "order_items", mode="append", properties=JDBC_PROPS)

    spark.stop()
    print("OK")


if __name__ == "__main__":
    main()