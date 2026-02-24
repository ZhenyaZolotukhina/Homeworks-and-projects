from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_HOST = "postgres"
PG_PORT = "5432"
PG_DB = "deliveries"
PG_USER = "postgres"
PG_PASSWORD = "postgres"

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}
POSTGRES_JAR = "/opt/airflow/postgresql.jar"


def build_spark():
    return (
        SparkSession.builder.appName("build_items_mart_agg")
        .master("local[2]")
        .config("spark.jars", POSTGRES_JAR)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )


def top_bottom_titles(base_df, period_col, prefix):
    # base_df columns must include: period_col, city, store_id, item_title, units_final, item_turnover

    w_desc = Window.partitionBy(period_col, "city", "store_id").orderBy(
        F.col("units_final").desc(), F.col("item_turnover").desc(), F.col("item_title").asc()
    )
    w_asc = Window.partitionBy(period_col, "city", "store_id").orderBy(
        F.col("units_final").asc(), F.col("item_turnover").asc(), F.col("item_title").asc()
    )

    most = (
        base_df
        .withColumn("rn", F.row_number().over(w_desc))
        .where(F.col("rn") == 1)
        .select(
            F.col(period_col).alias(period_col),
            "city",
            "store_id",
            F.col("item_title").alias(f"most_popular_item_title_{prefix}")
        )
    )

    least = (
        base_df
        .withColumn("rn", F.row_number().over(w_asc))
        .where(F.col("rn") == 1)
        .select(
            F.col(period_col).alias(period_col),
            "city",
            "store_id",
            F.col("item_title").alias(f"least_popular_item_title_{prefix}")
        )
    )

    return most.join(least, on=[period_col, "city", "store_id"], how="outer")


def main():
    spark = build_spark()

    orders_df = spark.read.jdbc(JDBC_URL, "orders", properties=JDBC_PROPS).select(
        "order_id", "store_id", "created_at", "address_text", "order_discount"
    )

    stores_df = spark.read.jdbc(JDBC_URL, "stores", properties=JDBC_PROPS).select(
        "store_id", "store_address"
    )

    items_df = spark.read.jdbc(JDBC_URL, "items", properties=JDBC_PROPS).select(
        "item_id", "item_title", "item_category"
    )

    order_items_df = spark.read.jdbc(JDBC_URL, "order_items", properties=JDBC_PROPS).select(
        "order_item_id",
        "order_id",
        "item_id",
        "item_quantity",
        "item_price",
        "item_discount",
        "item_canceled_quantity",
        "replaces_order_item_id",
    )

    replaced_parents = (
        order_items_df
        .select(F.col("replaces_order_item_id").alias("order_item_id"))
        .where(F.col("order_item_id").isNotNull())
        .dropDuplicates(["order_item_id"])
        .withColumn("is_replaced_parent", F.lit(1))
    )

    oi = (
        order_items_df
        .join(replaced_parents, on="order_item_id", how="left")
        .withColumn("is_replaced_parent", F.coalesce(F.col("is_replaced_parent"), F.lit(0)))
        .withColumn("item_quantity", F.col("item_quantity").cast("long"))
        .withColumn("item_canceled_quantity", F.coalesce(F.col("item_canceled_quantity").cast("long"), F.lit(0)))
        .withColumn("item_price", F.col("item_price").cast("double"))
        .withColumn("item_discount_rate", (F.coalesce(F.col("item_discount").cast("double"), F.lit(0.0)) / F.lit(100.0)))
        .withColumn("qty_effective", (F.col("item_quantity") - F.col("item_canceled_quantity")).cast("long"))
        .withColumn("qty_effective", F.when(F.col("qty_effective") < 0, 0).otherwise(F.col("qty_effective")))
        .withColumn("qty_final", F.when(F.col("is_replaced_parent") == 1, F.lit(0)).otherwise(F.col("qty_effective")))
    )

    o = (
        orders_df
        .withColumn("created_ts", F.to_timestamp("created_at"))
        .withColumn("order_discount_rate", (F.coalesce(F.col("order_discount").cast("double"), F.lit(0.0)) / F.lit(100.0)))
        .withColumn("city", F.regexp_extract(F.col("address_text"), r"^\s*([^\s,]+)", 1))
        .withColumn("year", F.year("created_ts"))
        .withColumn("month", F.month("created_ts"))
        .withColumn("day", F.dayofmonth("created_ts"))
        .withColumn("day_date", F.to_date("created_ts"))
        .withColumn("week_start", F.to_date(F.date_trunc("week", F.col("created_ts"))))
        .withColumn("month_start", F.to_date(F.date_trunc("month", F.col("created_ts"))))
        .select("order_id", "store_id", "city", "year", "month", "day", "day_date", "week_start", "month_start", "order_discount_rate")
    )

    fact = (
        oi.join(o, on="order_id", how="left")
          .join(stores_df, on="store_id", how="left")
          .join(items_df, on="item_id", how="left")
          .withColumn("line_gross", F.col("qty_final") * F.col("item_price"))
          .withColumn("line_item_discount", F.col("line_gross") * F.col("item_discount_rate"))
          .withColumn("line_net_after_item_discount", F.col("line_gross") - F.col("line_item_discount"))
          .withColumn("line_turnover", F.col("line_net_after_item_discount") * (F.lit(1.0) - F.coalesce(F.col("order_discount_rate"), F.lit(0.0))))
    )

    items_mart_agg = (
        fact.groupBy(
            "year", "month", "day", "city",
            "store_id", "store_address",
            "item_category", "item_id", "item_title"
        )
        .agg(
            F.sum("line_turnover").alias("item_turnover"),
            F.sum(F.col("item_quantity")).alias("units_ordered"),
            F.sum(F.col("item_canceled_quantity")).alias("units_canceled"),
            F.countDistinct("order_id").alias("orders_with_item"),
            F.countDistinct(F.when(F.col("item_canceled_quantity") > 0, F.col("order_id"))).alias("orders_with_item_cancel"),
            F.sum(F.col("qty_final")).alias("units_final"),
        )
        .withColumn("item_turnover", F.coalesce(F.col("item_turnover"), F.lit(0.0)))
        .withColumn("units_ordered", F.coalesce(F.col("units_ordered"), F.lit(0)))
        .withColumn("units_canceled", F.coalesce(F.col("units_canceled"), F.lit(0)))
        .withColumn("orders_with_item", F.coalesce(F.col("orders_with_item"), F.lit(0)))
        .withColumn("orders_with_item_cancel", F.coalesce(F.col("orders_with_item_cancel"), F.lit(0)))
        .withColumn("units_final", F.coalesce(F.col("units_final"), F.lit(0)))
        .withColumn("day_date", F.to_date(F.make_date(F.col("year"), F.col("month"), F.col("day"))))
        .withColumn("week_start", F.to_date(F.date_trunc("week", F.col("day_date"))))
        .withColumn("month_start", F.to_date(F.date_trunc("month", F.col("day_date"))))
    )

    base_day = (
        fact.groupBy("day_date", "city", "store_id", "item_title")
            .agg(
                F.sum("qty_final").alias("units_final"),
                F.sum("line_turnover").alias("item_turnover"),
            )
            .where(F.col("units_final") > 0)
    )
    base_week = (
        fact.groupBy("week_start", "city", "store_id", "item_title")
            .agg(
                F.sum("qty_final").alias("units_final"),
                F.sum("line_turnover").alias("item_turnover"),
            )
            .where(F.col("units_final") > 0)
    )
    base_month = (
        fact.groupBy("month_start", "city", "store_id", "item_title")
            .agg(
                F.sum("qty_final").alias("units_final"),
                F.sum("line_turnover").alias("item_turnover"),
            )
            .where(F.col("units_final") > 0)
    )

    pop_day = top_bottom_titles(base_day, "day_date", "day")
    pop_week = top_bottom_titles(base_week, "week_start", "week")
    pop_month = top_bottom_titles(base_month, "month_start", "month")

    items_mart_agg = (
        items_mart_agg
        .join(pop_day, on=["day_date", "city", "store_id"], how="left")
        .join(pop_week, on=["week_start", "city", "store_id"], how="left")
        .join(pop_month, on=["month_start", "city", "store_id"], how="left")
        .drop("day_date", "week_start", "month_start")
    )

    items_mart_agg.select(
        "year", "month", "day", "city",
        "store_id", "store_address",
        "item_category", "item_id", "item_title",
        "item_turnover", "units_ordered", "units_canceled",
        "orders_with_item", "orders_with_item_cancel",
        "most_popular_item_title_day", "least_popular_item_title_day",
        "most_popular_item_title_week", "least_popular_item_title_week",
        "most_popular_item_title_month", "least_popular_item_title_month",
    ).repartition(8).write.jdbc(
        JDBC_URL, "items_mart_agg", mode="overwrite", properties=JDBC_PROPS
    )

    print("items_mart_agg rows:")
    items_mart_agg.agg(F.count("*").alias("rows")).show()

    spark.stop()
    print("Items mart agg created successfully")


if __name__ == "__main__":
    main()