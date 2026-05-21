import os

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "bigdata_lab")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = os.getenv("CH_PORT", "8123")
CH_DB = os.getenv("CH_DB", "marts")

PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
CH_JDBC_URL = f"jdbc:ch://{CH_HOST}:{CH_PORT}/{CH_DB}"

PG_PROPS = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}

CH_PROPS = {
    "user": "default",
    "password": "",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}

SPARK_PACKAGES = ",".join(
    [
        "org.postgresql:postgresql:42.7.3",
        "com.clickhouse:clickhouse-jdbc:0.6.3",
    ]
)
