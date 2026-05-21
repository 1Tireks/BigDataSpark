"""
Spark ETL: star schema (PostgreSQL) -> 6 витрин (ClickHouse).
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from config import CH_DB, CH_JDBC_URL, CH_PROPS, PG_JDBC_URL, PG_PROPS


def read_pg(spark: SparkSession, table: str):
    return spark.read.jdbc(PG_JDBC_URL, table, properties=PG_PROPS)


def fill_nulls(df):
    """ClickHouse JDBC создаёт NOT NULL колонки — заполняем пропуски."""
    out = df
    for field in df.schema.fields:
        col = field.name
        dtype = field.dataType.simpleString()
        if dtype in ("int", "bigint", "double", "float", "long"):
            out = out.withColumn(col, F.coalesce(F.col(col), F.lit(0)))
        elif dtype == "string":
            out = out.withColumn(col, F.coalesce(F.col(col), F.lit("")))
    return out


def write_ch(df, table: str):
    (
        fill_nulls(df)
        .write.format("jdbc")
        .option("url", CH_JDBC_URL)
        .option("dbtable", table)
        .option("user", CH_PROPS["user"])
        .option("password", CH_PROPS["password"])
        .option("driver", CH_PROPS["driver"])
        .option("createTableOptions", "ENGINE=MergeTree ORDER BY report_part")
        .mode("overwrite")
        .save()
    )


def sales_enriched(spark: SparkSession):
    fact = read_pg(spark, "fact_sales").alias("f")
    customer = read_pg(spark, "dim_customer").alias("c")
    seller = read_pg(spark, "dim_seller").alias("s")
    product = read_pg(spark, "dim_product").alias("p")
    store = read_pg(spark, "dim_store").alias("st")
    supplier = read_pg(spark, "dim_supplier").alias("su")
    country = read_pg(spark, "dim_country")
    prod_cat = read_pg(spark, "dim_product_category").alias("pc")

    cc = country.select(
        F.col("country_id").alias("cust_country_id"),
        F.col("country_name").alias("customer_country"),
    )
    sc = country.select(
        F.col("country_id").alias("store_country_id"),
        F.col("country_name").alias("store_country"),
    )
    supc = country.select(
        F.col("country_id").alias("sup_country_id"),
        F.col("country_name").alias("supplier_country"),
    )

    return (
        fact.join(customer, F.col("f.customer_id") == F.col("c.customer_id"))
        .join(seller, F.col("f.seller_id") == F.col("s.seller_id"))
        .join(product, F.col("f.product_id") == F.col("p.product_id"))
        .join(prod_cat, F.col("p.category_id") == F.col("pc.product_category_id"), "left")
        .join(store, F.col("f.store_id") == F.col("st.store_id"), "left")
        .join(supplier, F.col("f.supplier_id") == F.col("su.supplier_id"), "left")
        .join(cc, F.col("c.country_id") == cc.cust_country_id, "left")
        .join(sc, F.col("st.country_id") == sc.store_country_id, "left")
        .join(supc, F.col("su.country_id") == supc.sup_country_id, "left")
        .select(
            F.col("f.sale_id"),
            F.col("f.sale_date"),
            F.col("f.customer_id"),
            F.col("f.seller_id"),
            F.col("f.product_id"),
            F.col("f.store_id"),
            F.col("f.supplier_id"),
            F.col("f.quantity"),
            F.col("f.total_price"),
            F.col("c.first_name"),
            F.col("c.last_name"),
            F.col("p.product_name"),
            F.col("p.price"),
            F.col("p.rating"),
            F.col("p.reviews"),
            F.col("pc.category_name"),
            F.col("st.name").alias("store_name"),
            F.col("st.city"),
            F.col("su.name").alias("supplier_name"),
            "customer_country",
            "store_country",
            "supplier_country",
        )
        .withColumn("sale_year", F.year("sale_date"))
        .withColumn("sale_month", F.date_format("sale_date", "yyyy-MM"))
    )


def with_report_cols(df, report_part: str):
    return df.withColumn("report_part", F.lit(report_part))


def mart_product_sales(sales):
    agg = sales.groupBy("product_id", "product_name", "category_name").agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("total_price").alias("revenue"),
        F.first("rating").alias("avg_rating"),
        F.first("reviews").alias("review_count"),
    )

    top10 = (
        agg.orderBy(F.col("units_sold").desc())
        .limit(10)
        .withColumn("rank", F.row_number().over(Window.orderBy(F.col("units_sold").desc())))
    )
    top10_out = with_report_cols(top10, "top10_products").select(
        "report_part",
        "rank",
        "product_name",
        F.col("category_name").alias("dimension_name"),
        "units_sold",
        "revenue",
        "avg_rating",
        "review_count",
        F.lit(None).cast("double").alias("metric_value"),
    )

    cat_rev = (
        agg.groupBy("category_name")
        .agg(F.sum("revenue").alias("revenue"))
        .withColumn("rank", F.lit(None).cast("int"))
    )
    cat_out = with_report_cols(cat_rev, "category_revenue").select(
        "report_part",
        "rank",
        F.lit(None).cast("string").alias("product_name"),
        F.col("category_name").alias("dimension_name"),
        F.lit(None).cast("long").alias("units_sold"),
        "revenue",
        F.lit(None).cast("double").alias("avg_rating"),
        F.lit(None).cast("long").alias("review_count"),
        F.lit(None).cast("double").alias("metric_value"),
    )

    ratings = agg.select(
        F.lit("product_rating").alias("report_part"),
        F.lit(None).cast("int").alias("rank"),
        "product_name",
        F.col("category_name").alias("dimension_name"),
        F.lit(None).cast("long").alias("units_sold"),
        F.lit(None).cast("double").alias("revenue"),
        "avg_rating",
        "review_count",
        F.lit(None).cast("double").alias("metric_value"),
    )

    return top10_out.unionByName(cat_out, allowMissingColumns=True).unionByName(
        ratings, allowMissingColumns=True
    )


def mart_customer_sales(sales):
    cust = sales.groupBy(
        "customer_id",
        "first_name",
        "last_name",
        "customer_country",
    ).agg(
        F.sum("total_price").alias("revenue"),
        F.count("*").alias("order_count"),
        (F.sum("total_price") / F.count("*")).alias("avg_check"),
    )

    top10 = (
        cust.orderBy(F.col("revenue").desc())
        .limit(10)
        .withColumn("rank", F.row_number().over(Window.orderBy(F.col("revenue").desc())))
    )
    top10_out = with_report_cols(top10, "top10_customers").select(
        "report_part",
        "rank",
        F.concat_ws(" ", "first_name", "last_name").alias("entity_name"),
        F.col("customer_country").alias("dimension_name"),
        F.lit(None).cast("long").alias("units_sold"),
        "revenue",
        F.lit(None).cast("double").alias("avg_rating"),
        "order_count",
        "avg_check",
    )

    by_country = cust.groupBy("customer_country").agg(
        F.countDistinct("customer_id").alias("customer_count"),
        F.sum("revenue").alias("revenue"),
    )
    country_out = with_report_cols(by_country, "customers_by_country").select(
        "report_part",
        F.lit(None).cast("int").alias("rank"),
        F.lit(None).cast("string").alias("entity_name"),
        F.col("customer_country").alias("dimension_name"),
        "customer_count",
        "revenue",
        F.lit(None).cast("double").alias("avg_rating"),
        F.lit(None).cast("long").alias("order_count"),
        F.lit(None).cast("double").alias("avg_check"),
    )

    avg_checks = with_report_cols(cust, "customer_avg_check").select(
        "report_part",
        F.lit(None).cast("int").alias("rank"),
        F.concat_ws(" ", "first_name", "last_name").alias("entity_name"),
        F.col("customer_country").alias("dimension_name"),
        F.lit(None).cast("long").alias("customer_count"),
        F.lit(None).cast("double").alias("revenue"),
        F.lit(None).cast("double").alias("avg_rating"),
        "order_count",
        "avg_check",
    )

    return top10_out.unionByName(country_out, allowMissingColumns=True).unionByName(
        avg_checks, allowMissingColumns=True
    )


def mart_time_sales(sales):
    monthly = sales.groupBy("sale_month", "sale_year").agg(
        F.sum("total_price").alias("revenue"),
        F.count("*").alias("order_count"),
        F.avg("total_price").alias("avg_order_size"),
    )

    monthly_out = with_report_cols(monthly, "monthly_trend").select(
        "report_part",
        F.lit(None).cast("int").alias("rank"),
        F.col("sale_month").alias("period"),
        F.col("sale_year").alias("year"),
        "revenue",
        "order_count",
        "avg_order_size",
        F.lit(None).cast("double").alias("prev_period_revenue"),
    )

    yearly = sales.groupBy("sale_year").agg(
        F.sum("total_price").alias("revenue"),
        F.count("*").alias("order_count"),
    )
    w = Window.orderBy("sale_year")
    yearly_cmp = yearly.withColumn(
        "prev_period_revenue", F.lag("revenue").over(w)
    ).withColumn(
        "revenue_change_pct",
        (F.col("revenue") - F.col("prev_period_revenue"))
        / F.col("prev_period_revenue")
        * 100,
    )
    yearly_out = with_report_cols(yearly_cmp, "yearly_comparison").select(
        "report_part",
        F.lit(None).cast("int").alias("rank"),
        F.col("sale_year").cast("string").alias("period"),
        "sale_year",
        "revenue",
        "order_count",
        F.lit(None).cast("double").alias("avg_order_size"),
        "prev_period_revenue",
    )

    avg_month = with_report_cols(monthly, "avg_order_by_month").select(
        "report_part",
        F.lit(None).cast("int").alias("rank"),
        "sale_month",
        "sale_year",
        F.lit(None).cast("double").alias("revenue"),
        F.lit(None).cast("long").alias("order_count"),
        "avg_order_size",
        F.lit(None).cast("double").alias("prev_period_revenue"),
    )

    return monthly_out.unionByName(yearly_out, allowMissingColumns=True).unionByName(
        avg_month, allowMissingColumns=True
    )


def mart_store_sales(sales):
    store_agg = sales.filter(F.col("store_id").isNotNull()).groupBy(
        "store_id", "store_name", "city", "store_country"
    ).agg(
        F.sum("total_price").alias("revenue"),
        F.count("*").alias("order_count"),
        (F.sum("total_price") / F.count("*")).alias("avg_check"),
    )

    top5 = (
        store_agg.orderBy(F.col("revenue").desc())
        .limit(5)
        .withColumn("rank", F.row_number().over(Window.orderBy(F.col("revenue").desc())))
    )
    top5_out = with_report_cols(top5, "top5_stores").select(
        "report_part",
        "rank",
        "store_name",
        "city",
        "store_country",
        "revenue",
        "order_count",
        "avg_check",
    )

    geo = store_agg.groupBy("city", "store_country").agg(
        F.sum("revenue").alias("revenue"),
        F.sum("order_count").alias("order_count"),
    )
    geo_out = with_report_cols(geo, "sales_by_city_country").select(
        "report_part",
        F.lit(None).cast("int").alias("rank"),
        F.lit(None).cast("string").alias("store_name"),
        "city",
        "store_country",
        "revenue",
        "order_count",
        F.lit(None).cast("double").alias("avg_check"),
    )

    avg_out = with_report_cols(store_agg, "store_avg_check").select(
        "report_part",
        F.lit(None).cast("int").alias("rank"),
        "store_name",
        "city",
        "store_country",
        "revenue",
        "order_count",
        "avg_check",
    )

    return top5_out.unionByName(geo_out, allowMissingColumns=True).unionByName(
        avg_out, allowMissingColumns=True
    )


def mart_supplier_sales(sales):
    sup = sales.filter(F.col("supplier_id").isNotNull()).groupBy(
        "supplier_id", "supplier_name", "supplier_country"
    ).agg(
        F.sum("total_price").alias("revenue"),
        F.avg("price").alias("avg_product_price"),
        F.count("*").alias("order_count"),
    )

    top5 = (
        sup.orderBy(F.col("revenue").desc())
        .limit(5)
        .withColumn("rank", F.row_number().over(Window.orderBy(F.col("revenue").desc())))
    )
    top5_out = with_report_cols(top5, "top5_suppliers").select(
        "report_part",
        "rank",
        "supplier_name",
        "supplier_country",
        "revenue",
        "avg_product_price",
        "order_count",
    )

    avg_price = with_report_cols(sup, "supplier_avg_price").select(
        "report_part",
        F.lit(None).cast("int").alias("rank"),
        "supplier_name",
        "supplier_country",
        F.lit(None).cast("double").alias("revenue"),
        "avg_product_price",
        "order_count",
    )

    by_country = sup.groupBy("supplier_country").agg(
        F.sum("revenue").alias("revenue"),
        F.sum("order_count").alias("order_count"),
    )
    country_out = with_report_cols(by_country, "sales_by_supplier_country").select(
        "report_part",
        F.lit(None).cast("int").alias("rank"),
        F.lit(None).cast("string").alias("supplier_name"),
        "supplier_country",
        "revenue",
        F.lit(None).cast("double").alias("avg_product_price"),
        "order_count",
    )

    return top5_out.unionByName(avg_price, allowMissingColumns=True).unionByName(
        country_out, allowMissingColumns=True
    )


def mart_product_quality(sales):
    prod = sales.groupBy("product_id", "product_name", "rating", "reviews").agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("total_price").alias("revenue"),
    )

    w_high = Window.orderBy(F.col("rating").desc(), F.col("reviews").desc())
    w_low = Window.orderBy(F.col("rating").asc(), F.col("reviews").desc())

    highest = (
        prod.withColumn("rank", F.row_number().over(w_high))
        .filter(F.col("rank") <= 10)
        .withColumn("report_part", F.lit("highest_rating"))
    )
    lowest = (
        prod.withColumn("rank", F.row_number().over(w_low))
        .filter(F.col("rank") <= 10)
        .withColumn("report_part", F.lit("lowest_rating"))
    )

    stats = prod.agg(
        F.corr("rating", "units_sold").alias("rating_sales_correlation"),
        F.count("*").alias("product_count"),
    )
    corr_out = stats.select(
        F.lit("rating_sales_correlation").alias("report_part"),
        F.lit(None).cast("int").alias("rank"),
        F.lit(None).cast("string").alias("product_name"),
        "rating_sales_correlation",
        F.lit(None).cast("double").alias("rating"),
        F.lit(None).cast("long").alias("reviews"),
        F.lit(None).cast("long").alias("units_sold"),
        F.lit(None).cast("double").alias("revenue"),
    )

    top_reviews = (
        prod.orderBy(F.col("reviews").desc())
        .limit(10)
        .withColumn("rank", F.row_number().over(Window.orderBy(F.col("reviews").desc())))
        .withColumn("report_part", F.lit("most_reviews"))
    )

    rated = highest.unionByName(lowest, allowMissingColumns=True).select(
        "report_part",
        "rank",
        "product_name",
        F.lit(None).cast("double").alias("rating_sales_correlation"),
        "rating",
        "reviews",
        "units_sold",
        "revenue",
    )
    reviewed = top_reviews.select(
        "report_part",
        "rank",
        "product_name",
        F.lit(None).cast("double").alias("rating_sales_correlation"),
        "rating",
        "reviews",
        "units_sold",
        "revenue",
    )

    return rated.unionByName(corr_out, allowMissingColumns=True).unionByName(
        reviewed, allowMissingColumns=True
    )


def main():
    spark = SparkSession.builder.appName("BigDataLab2-ClickHouseMarts").getOrCreate()

    sales = sales_enriched(spark)

    marts = {
        "mart_product_sales": mart_product_sales(sales),
        "mart_customer_sales": mart_customer_sales(sales),
        "mart_time_sales": mart_time_sales(sales),
        "mart_store_sales": mart_store_sales(sales),
        "mart_supplier_sales": mart_supplier_sales(sales),
        "mart_product_quality": mart_product_quality(sales),
    }

    for name, df in marts.items():
        write_ch(df, name)
        print(f"{name}: {df.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
