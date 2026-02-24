from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PG_HOST = "postgres"
PG_PORT = "5432"
PG_DB = "deliveries"
PG_USER = "postgres"
PG_PASSWORD = "postgres"

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}
POSTGRES_JAR = "/opt/airflow/postgresql.jar"


def main():
    spark = (
        SparkSession.builder.appName("orders_mart_agg")
        .master("local[2]")
        .config("spark.jars", POSTGRES_JAR)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )

    orders = spark.read.jdbc(JDBC_URL, "orders", properties=JDBC_PROPS)
    stores = spark.read.jdbc(JDBC_URL, "stores", properties=JDBC_PROPS)
    oi_raw = spark.read.jdbc(JDBC_URL, "order_items", properties=JDBC_PROPS)
    driver_asg = spark.read.jdbc(JDBC_URL, "driver_assignment", properties=JDBC_PROPS)

    replaced_parents = (
        oi_raw.select(F.col("replaces_order_item_id").alias("order_item_id"))
        .where(F.col("order_item_id").isNotNull())
        .dropDuplicates(["order_item_id"])
        .withColumn("is_replaced_parent", F.lit(1))
    )

    oi = (
        oi_raw.join(replaced_parents, on="order_item_id", how="left")
        .withColumn("is_replaced_parent", F.coalesce(F.col("is_replaced_parent"), F.lit(0)))
        .withColumn("item_quantity", F.col("item_quantity").cast("long"))
        .withColumn("item_canceled_quantity", F.coalesce(F.col("item_canceled_quantity").cast("long"), F.lit(0)))
        .withColumn("item_price", F.col("item_price").cast("double"))
        .withColumn("item_discount_rate", F.coalesce(F.col("item_discount").cast("double"), F.lit(0.0)) / F.lit(100.0))
    )

    oi = oi.withColumn(
        "qty_turnover",
        F.when(F.col("is_replaced_parent") == 1, F.lit(0)).otherwise(F.col("item_quantity"))
    )

    oi = oi.withColumn(
        "qty_revenue",
        (F.col("item_quantity") - F.col("item_canceled_quantity")).cast("long")
    )
    oi = oi.withColumn("qty_revenue", F.when(F.col("qty_revenue") < 0, 0).otherwise(F.col("qty_revenue")))
    oi = oi.withColumn(
        "qty_revenue",
        F.when(F.col("is_replaced_parent") == 1, F.lit(0)).otherwise(F.col("qty_revenue"))
    )

    oi = oi.withColumn("line_turnover", F.col("qty_turnover") * F.col("item_price") * (F.lit(1.0) - F.col("item_discount_rate")))
    oi = oi.withColumn("line_revenue",  F.col("qty_revenue")  * F.col("item_price") * (F.lit(1.0) - F.col("item_discount_rate")))

    oi_order = (
        oi.groupBy("order_id")
        .agg(
            F.sum("line_turnover").alias("items_turnover_after_item_discounts"),
            F.sum("line_revenue").alias("items_revenue_after_item_discounts_and_replacements_and_cancels"),
        )
    )

    driver_changes = (
        driver_asg.groupBy("order_id")
        .agg(F.countDistinct("driver_id").alias("drivers_assigned_cnt"))
        .withColumn("has_driver_change", F.when(F.col("drivers_assigned_cnt") > 1, 1).otherwise(0))
    )

    o = (
        orders.join(oi_order, "order_id", "left")
              .join(driver_changes, "order_id", "left")
              .join(stores, "store_id", "left")
    )

    o = (
        o.withColumn("created_ts", F.to_timestamp("created_at"))
         .withColumn("year",  F.year("created_ts"))
         .withColumn("month", F.month("created_ts"))
         .withColumn("day",   F.dayofmonth("created_ts"))
         .withColumn("city", F.regexp_extract(F.col("address_text"), r"^\s*([^\s,]+)", 1))
    )

    o = o.withColumn(
        "order_status",
        F.when(F.col("canceled_at").isNotNull(), F.lit("canceled"))
         .when(F.col("delivered_at").isNotNull(), F.lit("delivered"))
         .when(F.col("delivery_started_at").isNotNull(), F.lit("in_delivery"))
         .when(F.col("paid_at").isNotNull(), F.lit("paid"))
         .otherwise(F.lit("created"))
    )

    o = o.withColumn("order_discount_rate", F.coalesce(F.col("order_discount").cast("double"), F.lit(0.0)) / F.lit(100.0))
    delivery_cost = F.coalesce(F.col("delivery_cost").cast("double"), F.lit(0.0))

    items_turnover = F.coalesce(F.col("items_turnover_after_item_discounts"), F.lit(0.0))
    items_revenue  = F.coalesce(F.col("items_revenue_after_item_discounts_and_replacements_and_cancels"), F.lit(0.0))

    o = o.withColumn("turnover_order", items_turnover * (F.lit(1.0) - F.col("order_discount_rate")))

    o = o.withColumn(
        "revenue_order",
        F.when(F.col("paid_at").isNotNull(), items_revenue * (F.lit(1.0) - F.col("order_discount_rate")) + delivery_cost)
         .otherwise(F.lit(0.0))
    )

    o = o.withColumn(
        "expenses_order",
        F.when(F.col("delivered_at").isNotNull(), delivery_cost).otherwise(F.lit(0.0))
    )

    o = o.withColumn("profit_order", F.col("revenue_order") - F.col("expenses_order"))

    grp = ["year", "month", "day", "city", "store_id", "store_address"]

    base = o.select(
        "order_id", "user_id", "driver_id",
        "year", "month", "day", "city", "store_id", "store_address",
        "paid_at", "delivered_at", "canceled_at", "order_cancellation_reason",
        "has_driver_change",
        "turnover_order", "revenue_order", "profit_order"
    )

    orders_mart = (
        base.groupBy(*grp)
        .agg(

            F.sum("turnover_order").alias("turnover"),
            F.sum("revenue_order").alias("revenue"),
            F.sum("profit_order").alias("profit"),

            F.countDistinct("order_id").alias("orders_created"),
            F.countDistinct(F.when(F.col("delivered_at").isNotNull(), F.col("order_id"))).alias("orders_delivered"),
            F.countDistinct(F.when(F.col("canceled_at").isNotNull(), F.col("order_id"))).alias("orders_canceled"),
            F.countDistinct(F.when((F.col("canceled_at").isNotNull()) & (F.col("delivered_at").isNotNull()), F.col("order_id"))).alias("orders_canceled_after_delivery"),
            F.countDistinct(F.when((F.col("canceled_at").isNotNull()) & (F.col("order_cancellation_reason").isin("Ошибка приложения", "Проблемы с оплатой")), F.col("order_id"))).alias("orders_canceled_service_error"),

            F.countDistinct("user_id").alias("buyers"),

            F.countDistinct(F.when(F.col("has_driver_change") == 1, F.col("order_id"))).alias("orders_with_driver_change"),

            F.countDistinct(F.when(F.col("delivered_at").isNotNull(), F.col("driver_id"))).alias("active_drivers"),

            F.countDistinct(F.when(F.col("paid_at").isNotNull(), F.col("order_id"))).alias("orders_paid"),
        )
    )

    orders_mart = (
        orders_mart
        .withColumn("avg_check", F.when(F.col("orders_paid") > 0, F.col("revenue") / F.col("orders_paid")))
        .withColumn("orders_per_buyer", F.when(F.col("buyers") > 0, F.col("orders_created") / F.col("buyers")))
        .withColumn("revenue_per_buyer", F.when(F.col("buyers") > 0, F.col("revenue") / F.col("buyers")))
        .drop("orders_paid")
    )

    orders_mart.repartition(8).write.jdbc(
        JDBC_URL,
        "orders_mart_agg",
        mode="overwrite",
        properties=JDBC_PROPS,
    )

    print("orders_mart_agg sample:")
    orders_mart.orderBy(F.desc("revenue")).show(10, truncate=False)

    spark.stop()
    print("OK: orders_mart_agg created")


if __name__ == "__main__":
    main()