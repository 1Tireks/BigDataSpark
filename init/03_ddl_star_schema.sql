-- Пустая схема «звезда»; заполнение — Spark job etl_star_schema.py
CREATE TABLE dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_pet_category (
    pet_category_id SERIAL PRIMARY KEY,
    category_name   VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE dim_product_category (
    product_category_id SERIAL PRIMARY KEY,
    category_name       VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_pet (
    pet_id          SERIAL PRIMARY KEY,
    pet_type        VARCHAR(50),
    pet_name        VARCHAR(100),
    pet_breed       VARCHAR(100),
    pet_category_id INTEGER REFERENCES dim_pet_category (pet_category_id),
    UNIQUE (pet_type, pet_name, pet_breed, pet_category_id)
);

CREATE TABLE dim_customer (
    customer_id        SERIAL PRIMARY KEY,
    source_customer_id INTEGER NOT NULL UNIQUE,
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    age                INTEGER,
    email              VARCHAR(255),
    postal_code        VARCHAR(20),
    country_id         INTEGER REFERENCES dim_country (country_id),
    pet_id             INTEGER REFERENCES dim_pet (pet_id)
);

CREATE TABLE dim_seller (
    seller_id        SERIAL PRIMARY KEY,
    source_seller_id INTEGER NOT NULL UNIQUE,
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    email            VARCHAR(255),
    postal_code      VARCHAR(20),
    country_id       INTEGER REFERENCES dim_country (country_id)
);

CREATE TABLE dim_product (
    product_id        SERIAL PRIMARY KEY,
    source_product_id INTEGER NOT NULL UNIQUE,
    product_name      VARCHAR(255),
    price             NUMERIC(10, 2),
    weight            NUMERIC(10, 2),
    color             VARCHAR(50),
    size              VARCHAR(50),
    brand             VARCHAR(100),
    material          VARCHAR(100),
    description       TEXT,
    rating            NUMERIC(3, 1),
    reviews           INTEGER,
    release_date      DATE,
    expiry_date       DATE,
    category_id       INTEGER REFERENCES dim_product_category (product_category_id)
);

CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name        VARCHAR(150),
    contact     VARCHAR(150),
    email       VARCHAR(255),
    phone       VARCHAR(30),
    address     VARCHAR(255),
    city        VARCHAR(100),
    country_id  INTEGER REFERENCES dim_country (country_id),
    UNIQUE (name, email, phone)
);

CREATE TABLE dim_store (
    store_id   SERIAL PRIMARY KEY,
    name       VARCHAR(150),
    location   VARCHAR(150),
    city       VARCHAR(100),
    state      VARCHAR(100),
    phone      VARCHAR(30),
    email      VARCHAR(255),
    country_id INTEGER REFERENCES dim_country (country_id),
    UNIQUE (name, email, phone)
);

CREATE TABLE fact_sales (
    sale_id       SERIAL PRIMARY KEY,
    source_row_id INTEGER,
    sale_date     DATE NOT NULL,
    customer_id   INTEGER NOT NULL REFERENCES dim_customer (customer_id),
    seller_id     INTEGER NOT NULL REFERENCES dim_seller (seller_id),
    product_id    INTEGER NOT NULL REFERENCES dim_product (product_id),
    store_id      INTEGER REFERENCES dim_store (store_id),
    supplier_id   INTEGER REFERENCES dim_supplier (supplier_id),
    quantity      INTEGER NOT NULL,
    total_price   NUMERIC(10, 2) NOT NULL
);
