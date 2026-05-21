"""
Spark ETL: mock_data (PostgreSQL) -> star schema (PostgreSQL).
Аналог лабораторной №1, но трансформация выполняется в Spark.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from config import PG_JDBC_URL, PG_PROPS


def read_mock(spark: SparkSession):
    return (
        spark.read.jdbc(PG_JDBC_URL, "mock_data", properties=PG_PROPS)
        .withColumn(
            "sale_date_parsed",
            F.to_date(F.col("sale_date"), "M/d/yyyy"),
        )
        .withColumn(
            "product_release_date_parsed",
            F.to_date(F.col("product_release_date"), "M/d/yyyy"),
        )
        .withColumn(
            "product_expiry_date_parsed",
            F.to_date(F.col("product_expiry_date"), "M/d/yyyy"),
        )
    )


def nonempty(col_name: str):
    return (F.col(col_name).isNotNull()) & (F.trim(F.col(col_name)) != "")


def write_table(df, table: str):
    (
        df.write.format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("dbtable", table)
        .option("user", PG_PROPS["user"])
        .option("password", PG_PROPS["password"])
        .option("driver", PG_PROPS["driver"])
        .mode("append")
        .save()
    )


def build_dim_country(mock):
    countries = (
        mock.filter(nonempty("customer_country"))
        .select(F.col("customer_country").alias("country_name"))
        .unionByName(
            mock.filter(nonempty("seller_country")).select(
                F.col("seller_country").alias("country_name")
            )
        )
        .unionByName(
            mock.filter(nonempty("store_country")).select(
                F.col("store_country").alias("country_name")
            )
        )
        .unionByName(
            mock.filter(nonempty("supplier_country")).select(
                F.col("supplier_country").alias("country_name")
            )
        )
        .distinct()
    )
    w = Window.orderBy("country_name")
    return countries.withColumn("country_id", F.row_number().over(w))


def build_dim_pet_category(mock):
    w = Window.orderBy("category_name")
    return (
        mock.filter(nonempty("pet_category"))
        .select(F.col("pet_category").alias("category_name"))
        .distinct()
        .withColumn("pet_category_id", F.row_number().over(w))
    )


def build_dim_product_category(mock):
    w = Window.orderBy("category_name")
    return (
        mock.filter(nonempty("product_category"))
        .select(F.col("product_category").alias("category_name"))
        .distinct()
        .withColumn("product_category_id", F.row_number().over(w))
    )


def build_dim_pet(mock, dim_pet_category):
    pets = (
        mock.select(
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
            "pet_category",
        )
        .distinct()
        .join(
            dim_pet_category,
            F.col("pet_category") == F.col("category_name"),
            "left",
        )
        .select(
            F.col("customer_pet_type").alias("pet_type"),
            F.col("customer_pet_name").alias("pet_name"),
            F.col("customer_pet_breed").alias("pet_breed"),
            "pet_category_id",
        )
        .distinct()
    )
    w = Window.orderBy("pet_type", "pet_name", "pet_breed", "pet_category_id")
    return pets.withColumn("pet_id", F.row_number().over(w))


def build_dim_customer(mock, dim_country, dim_pet, dim_pet_category):
    w = Window.partitionBy("sale_customer_id").orderBy("id")
    ranked = mock.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)

    dc = dim_country.select(
        F.col("country_name").alias("customer_country"),
        "country_id",
    )
    dp = dim_pet.join(
        dim_pet_category.select(
            F.col("pet_category_id"),
            F.col("category_name").alias("pet_cat_name"),
        ),
        "pet_category_id",
        "left",
    ).select(
        "pet_id",
        "pet_type",
        "pet_name",
        "pet_breed",
        "pet_cat_name",
    )

    return (
        ranked.join(dc, "customer_country", "left")
        .join(
            dp,
            (F.col("customer_pet_type") == F.col("pet_type"))
            & (F.col("customer_pet_name") == F.col("pet_name"))
            & (F.col("customer_pet_breed") == F.col("pet_breed"))
            & (F.col("pet_category") == F.col("pet_cat_name")),
            "left",
        )
        .select(
            F.col("sale_customer_id").alias("source_customer_id"),
            F.col("customer_first_name").alias("first_name"),
            F.col("customer_last_name").alias("last_name"),
            F.col("customer_age").alias("age"),
            F.col("customer_email").alias("email"),
            F.when(nonempty("customer_postal_code"), F.col("customer_postal_code"))
            .otherwise(F.lit(None))
            .alias("postal_code"),
            "country_id",
            "pet_id",
        )
        .distinct()
        .withColumn(
            "customer_id",
            F.row_number().over(Window.orderBy("source_customer_id")),
        )
    )


def build_dim_seller(mock, dim_country):
    w = Window.partitionBy("sale_seller_id").orderBy("id")
    ranked = mock.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
    ds = dim_country.select(
        F.col("country_name").alias("seller_country"),
        "country_id",
    )
    return (
        ranked.join(ds, "seller_country", "left")
        .select(
            F.col("sale_seller_id").alias("source_seller_id"),
            F.col("seller_first_name").alias("first_name"),
            F.col("seller_last_name").alias("last_name"),
            F.col("seller_email").alias("email"),
            F.when(nonempty("seller_postal_code"), F.col("seller_postal_code"))
            .otherwise(F.lit(None))
            .alias("postal_code"),
            "country_id",
        )
        .distinct()
        .withColumn(
            "seller_id", F.row_number().over(Window.orderBy("source_seller_id"))
        )
    )


def build_dim_product(mock, dim_product_category):
    w = Window.partitionBy("sale_product_id").orderBy("id")
    ranked = mock.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
    dpc = dim_product_category.select(
        F.col("category_name").alias("product_category"),
        F.col("product_category_id").alias("category_id"),
    )
    return (
        ranked.join(dpc, "product_category", "left")
        .select(
            F.col("sale_product_id").alias("source_product_id"),
            F.col("product_name"),
            F.col("product_price").alias("price"),
            F.col("product_weight").alias("weight"),
            F.col("product_color").alias("color"),
            F.col("product_size").alias("size"),
            F.col("product_brand").alias("brand"),
            F.col("product_material").alias("material"),
            F.col("product_description").alias("description"),
            F.col("product_rating").alias("rating"),
            F.col("product_reviews").alias("reviews"),
            F.col("product_release_date_parsed").alias("release_date"),
            F.col("product_expiry_date_parsed").alias("expiry_date"),
            "category_id",
        )
        .distinct()
        .withColumn(
            "product_id", F.row_number().over(Window.orderBy("source_product_id"))
        )
    )


def build_dim_supplier(mock, dim_country):
    dsc = dim_country.select(
        F.col("country_name").alias("supplier_country"),
        "country_id",
    )
    suppliers = (
        mock.select(
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        )
        .distinct()
        .join(dsc, "supplier_country", "left")
        .select(
            F.col("supplier_name").alias("name"),
            F.col("supplier_contact").alias("contact"),
            F.col("supplier_email").alias("email"),
            F.col("supplier_phone").alias("phone"),
            F.col("supplier_address").alias("address"),
            F.col("supplier_city").alias("city"),
            "country_id",
        )
        .distinct()
    )
    return suppliers.withColumn(
        "supplier_id", F.row_number().over(Window.orderBy("name", "email"))
    )


def build_dim_store(mock, dim_country):
    dsc = dim_country.select(
        F.col("country_name").alias("store_country"),
        "country_id",
    )
    stores = (
        mock.select(
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_phone",
            "store_email",
            "store_country",
        )
        .distinct()
        .join(dsc, "store_country", "left")
        .select(
            F.col("store_name").alias("name"),
            F.col("store_location").alias("location"),
            F.col("store_city").alias("city"),
            F.col("store_state").alias("state"),
            F.col("store_phone").alias("phone"),
            F.col("store_email").alias("email"),
            "country_id",
        )
        .distinct()
    )
    return stores.withColumn(
        "store_id", F.row_number().over(Window.orderBy("name", "email"))
    )


def build_fact_sales(
    mock,
    dim_customer,
    dim_seller,
    dim_product,
    dim_store,
    dim_supplier,
):
    dc = dim_customer.select(
        F.col("source_customer_id"),
        F.col("customer_id"),
    )
    ds = dim_seller.select(F.col("source_seller_id"), F.col("seller_id"))
    dp = dim_product.select(F.col("source_product_id"), F.col("product_id"))
    dst = dim_store.select("name", "email", "phone", "store_id")
    dsup = dim_supplier.select("name", "email", "phone", "supplier_id")

    mock = mock.withColumn("source_row_id", F.col("id"))

    joined = (
        mock.join(dc, mock.sale_customer_id == dc.source_customer_id)
        .join(ds, mock.sale_seller_id == ds.source_seller_id)
        .join(dp, mock.sale_product_id == dp.source_product_id)
        .join(
            dst,
            (mock.store_name == dst.name)
            & (mock.store_email == dst.email)
            & (mock.store_phone == dst.phone),
            "left",
        )
        .join(
            dsup,
            (mock.supplier_name == dsup.name)
            & (mock.supplier_email == dsup.email)
            & (mock.supplier_phone == dsup.phone),
            "left",
        )
    )

    w = Window.orderBy("source_row_id")
    return joined.select(
        "source_row_id",
        F.col("sale_date_parsed").alias("sale_date"),
        "customer_id",
        "seller_id",
        "product_id",
        "store_id",
        "supplier_id",
        F.col("sale_quantity").alias("quantity"),
        F.col("sale_total_price").alias("total_price"),
    ).withColumn("sale_id", F.row_number().over(w))


def main():
    spark = (
        SparkSession.builder.appName("BigDataLab2-StarSchema")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

    mock = read_mock(spark)

    dim_country = build_dim_country(mock)
    dim_pet_category = build_dim_pet_category(mock)
    dim_product_category = build_dim_product_category(mock)
    dim_pet = build_dim_pet(mock, dim_pet_category)
    dim_customer = build_dim_customer(mock, dim_country, dim_pet, dim_pet_category)
    dim_seller = build_dim_seller(mock, dim_country)
    dim_product = build_dim_product(mock, dim_product_category)
    dim_supplier = build_dim_supplier(mock, dim_country)
    dim_store = build_dim_store(mock, dim_country)
    fact_sales = build_fact_sales(
        mock, dim_customer, dim_seller, dim_product, dim_store, dim_supplier
    )

    # Порядок: измерения без FK -> с FK -> факт
    write_table(dim_country, "dim_country")
    write_table(dim_pet_category, "dim_pet_category")
    write_table(dim_product_category, "dim_product_category")
    write_table(dim_pet, "dim_pet")
    write_table(dim_customer, "dim_customer")
    write_table(dim_seller, "dim_seller")
    write_table(dim_product, "dim_product")
    write_table(dim_supplier, "dim_supplier")
    write_table(dim_store, "dim_store")
    write_table(fact_sales, "fact_sales")

    cnt = fact_sales.count()
    print(f"fact_sales rows written: {cnt}")
    spark.stop()


if __name__ == "__main__":
    main()
