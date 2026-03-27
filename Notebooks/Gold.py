
# GLOBALMART — GOLD LAYER (Dimensional model + facts + aggregations)

# What this about
# - Dimensions: `dim_region`, `dim_date`, `dim_customer`, `dim_product`, `dim_vendor`
# - Facts: `fact_sales`, `fact_return`
# - Pre-aggregations: `agg_monthly_revenue_by_region`, `agg_customer_return_summary`, `agg_vendor_return_rate`, `agg_product_region_monthly`

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Source (cleaned layer)
SILVER = "globalmart.silver"

# Target (analytical layer)
GOLD_CAT = "globalmart.gold"

# HELPER FUNCTION — REGION SURROGATE KEY
def region_key_expr(col):
    c = F.trim(col)
    return (
        F.when(c.isin("East"), F.lit(1))
        .when(c.isin("West"), F.lit(2))
        .when(c.isin("North"), F.lit(3))
        .when(c.isin("South"), F.lit(4))
        .when(c.isin("Central"), F.lit(5))
        .otherwise(F.lit(99))
    )

# DIMENSION: REGION
@dlt.table(
    name=f"{GOLD_CAT}.dim_region",
    comment="Region dimension — standardized list of regions for analysis"
)
def dim_region():
    """
    SOURCE:
    - globalmart.silver.customers

    OUTPUT:
    - region_key (PK)
    - region_name
    """

    c = spark.table(f"{SILVER}.customers")

    # Extract unique regions from customer data
    reg = (
        c.select(F.trim(F.col("region")).alias("region_name"))
        .distinct()
        .filter(F.col("region_name").isNotNull() & (F.col("region_name") != ""))
    )

    # Add surrogate key for efficient joins
    return reg.withColumn("region_key", region_key_expr(F.col("region_name")))

# DIMENSION: DATE
@dlt.table(
    name=f"{GOLD_CAT}.dim_date",
    comment="Date dimension — calendar attributes for orders and returns"
)
def dim_date():
    """
    SOURCE:
    - globalmart.silver.orders
    - globalmart.silver.returns

    OUTPUT:
    - date_key (YYYYMMDD integer)
    - calendar_date
    - year, quarter, month
    - month_name
    - year_month (used in aggregations)
    """

    o = spark.table(f"{SILVER}.orders")
    r = spark.table(f"{SILVER}.returns")

    # Extract order dates
    od = o.select(
        F.to_date(F.col("order_purchase_date")).alias("calendar_date")
    ).filter(F.col("calendar_date").isNotNull())

    # Extract return dates
    rd = r.select(
        F.to_date(F.col("return_date")).alias("calendar_date")
    ).filter(F.col("calendar_date").isNotNull())

    # Combine all unique dates
    dates = od.unionByName(rd).distinct()

    # Generate calendar attributes
    return (
        dates.withColumn(
            "date_key",
            F.date_format(F.col("calendar_date"), "yyyyMMdd").cast("int")
        )
        .withColumn("year", F.year("calendar_date"))
        .withColumn("quarter", F.quarter("calendar_date"))
        .withColumn("month", F.month("calendar_date"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
        .withColumn("year_month", F.date_format("calendar_date", "yyyy-MM"))
    )

# DIMENSION: CUSTOMER

@dlt.table(
    name=f"{GOLD_CAT}.dim_customer",
    comment="Customer dimension — current state (Type 1) with attributes for segmentation and regional analysis"
)
def dim_customer():
    """
    SOURCE:
    - globalmart.silver.customers

    OUTPUT:
    - customer_id (PK)
    - customer_email, customer_name
    - customer_segment
    - region_name, region_key
    - address attributes (country, city, state, postal_code)
    """

    c = spark.table(f"{SILVER}.customers")

    return c.select(
        F.col("customer_id"),
        F.col("customer_email"),
        F.col("customer_name"),

        # Standardized segment for analysis
        F.col("segment").alias("customer_segment"),

        # Region (string + surrogate key)
        F.trim(F.col("region")).alias("region_name"),
        region_key_expr(F.col("region")).alias("region_key"),

        # Additional attributes for filtering
        F.col("country"),
        F.col("city"),
        F.col("state"),
        F.col("postal_code"),
    )

# DIMENSION: PRODUCT
@dlt.table(
    name=f"{GOLD_CAT}.dim_product",
    comment="Product dimension — product catalogue for sales and inventory analysis"
)
def dim_product():
    """
    SOURCE:
    - globalmart.silver.products

    OUTPUT:
    - product_id (PK)
    - product attributes (name, category, etc.)
    """

    p = spark.table(f"{SILVER}.products")

    # Remove pipeline/technical columns
    drop_cols = {"_source_file_path", "bronze_ingest_ts", "silver_load_ts"}

    cols = [x for x in p.columns if x not in drop_cols]

    return p.select(*cols) if cols else p

# DIMENSION: VENDOR
@dlt.table(
    name=f"{GOLD_CAT}.dim_vendor",
    comment="Vendor dimension — supplier reference for return rate and performance analysis"
)
def dim_vendor():
    """
    SOURCE:
    - globalmart.silver.vendors

    OUTPUT:
    - vendor_id (PK)
    - vendor_name
    """

    v = spark.table(f"{SILVER}.vendors")

    return v.select(
        # Clean vendor_id and vendor_name
        F.trim(F.col("vendor_id")).alias("vendor_id"),
        F.trim(F.col("vendor_name")).alias("vendor_name"),
    )

# FACT TABLE: SALES

@dlt.table(
    name=f"{GOLD_CAT}.fact_sales",
    comment="Fact table — line-level sales (grain: one row per transaction line)"
)
def fact_sales():
    """
    GRAIN:
    - One row per transaction line (order_id + product_id)

    SOURCE:
    - globalmart.silver.transactions (sales + quantity + profit)
    - globalmart.silver.orders (order date, vendor)
    - globalmart.silver.customers (segment, region)

    DIMENSION KEYS:
    - customer_id → dim_customer
    - product_id → dim_product
    - vendor_id → dim_vendor
    - region_key → dim_region
    - date_key → dim_date
    """

    t = spark.table(f"{SILVER}.transactions")
    o = spark.table(f"{SILVER}.orders")
    c = spark.table(f"{SILVER}.customers")

    # JOIN LOGIC
    # Join transactions with orders to get order date and vendor_id
    # Join with customers to get region and segment
    j = (
        t.join(o, "order_id", "inner")
        .join(c, "customer_id", "inner")
        .select(
            F.col("order_id"),
            F.col("product_id"),
            F.col("sales_amount"),
            F.col("quantity"),
            F.col("discount"),
            F.col("profit"),
            F.col("profit_missing_flag"),
            F.col("vendor_id"),
            F.col("order_purchase_date"),
            F.col("customer_id"),

            # Customer attributes for analysis
            F.trim(F.col("region")).alias("customer_region"),
            F.col("segment").alias("customer_segment"),
        )
    )

    # DERIVED COLUMNS

    j = j.withColumn(
        "order_date",
        F.to_date(F.col("order_purchase_date"))
    ).withColumn(
        # Used to join with dim_date
        "date_key",
        F.date_format(F.col("order_date"), "yyyyMMdd").cast("int"),
    ).withColumn(
        # Used for aggregations (monthly)
        "year_month",
        F.date_format(F.col("order_date"), "yyyy-MM"),
    ).withColumn(
        # Region surrogate key for joining dim_region
        "region_key",
        region_key_expr(F.col("customer_region")),
    ).withColumn(
        # Derived measure: discount amount
        "discount_amount",
        F.coalesce(F.col("sales_amount") * F.col("discount"), F.lit(0.0)),
    )

    # SURROGATE KEY (FACT PRIMARY KEY)
    # Unique identifier for each transaction line
    line_key = F.sha2(
        F.concat_ws(
            "|",
            F.col("order_id"),
            F.col("product_id"),
            F.col("sales_amount").cast("string"),
            F.col("quantity").cast("string"),
            F.coalesce(F.col("profit"), F.lit(0.0)).cast("string"),
        ),
        256,
    )

    # FINAL OUTPUT

    return j.select(
        line_key.alias("sales_line_key"),
        "order_id",
        "order_date",
        "date_key",
        "year_month",
        "customer_id",
        "customer_segment",
        "product_id",
        "vendor_id",
        "region_key",

        # Measures
        "sales_amount",
        F.col("quantity").cast(DoubleType()).alias("quantity"),
        F.col("profit").cast(DoubleType()).alias("profit"),
        "profit_missing_flag",
        F.col("discount").alias("discount_rate"),
        "discount_amount",
    )

# FACT TABLE: RETURNS

@dlt.table(
    name=f"{GOLD_CAT}.fact_return",
    comment="Fact table — return events (grain: one row per return)"
)
def fact_return():
    """
    GRAIN:
    - One row per return event

    SOURCE:
    - globalmart.silver.returns
    - globalmart.silver.orders
    - globalmart.silver.customers

    DIMENSION KEYS:
    - customer_id → dim_customer
    - vendor_id → dim_vendor
    - region_key → dim_region
    - return_date_key → dim_date
    """

    r = spark.table(f"{SILVER}.returns")
    o = spark.table(f"{SILVER}.orders")
    c = spark.table(f"{SILVER}.customers")

    # JOIN LOGIC
    # Join returns with orders to get vendor_id
    # Join with customers to get region
    j = (
        r.join(o, "order_id", "inner")
        .join(c, "customer_id", "inner")
        .select(
            F.col("order_id"),
            F.col("return_reason"),
            F.col("refund_amount"),
            F.col("return_date"),
            F.col("return_status"),
            F.col("vendor_id"),
            F.col("customer_id"),
            F.trim(F.col("region")).alias("customer_region"),
        )
    )

    # DERIVED COLUMNS

    j = j.withColumn(
        "return_dt",
        F.to_date(F.col("return_date"))
    ).withColumn(
        # Used to join with dim_date
        "return_date_key",
        F.date_format(F.col("return_dt"), "yyyyMMdd").cast("int"),
    ).withColumn(
        # Region surrogate key
        "region_key",
        region_key_expr(F.col("customer_region"))
    )

    # SURROGATE KEY (FACT PRIMARY KEY)

    rk = F.sha2(
        F.concat_ws(
            "|",
            F.col("order_id"),
            F.col("refund_amount").cast("string"),
            F.col("return_dt").cast("string"),
            F.col("return_status"),
        ),
        256,
    )

    # FINAL OUTPUT

    return j.select(
        rk.alias("return_key"),
        "order_id",
        "return_reason",
        F.col("refund_amount").cast(DoubleType()).alias("refund_amount"),
        F.col("return_dt").alias("return_date"),
        "return_date_key",
        "return_status",
        "vendor_id",
        "customer_id",
        "region_key",
    )

# GOLD LAYER — PRE-AGGREGATIONS

# AGGREGATION: MONTHLY REVENUE BY REGION

@dlt.table(
    name=f"{GOLD_CAT}.agg_monthly_revenue_by_region",
    comment="Monthly revenue metrics by region (sales, profit, order count)"
)
def agg_monthly_revenue_by_region():
    """
    BUSINESS PURPOSE:
    - Revenue audit and performance tracking
    - Used in dashboards to analyze monthly trends by region

    SOURCE:
    - fact_sales (transaction-level data)
    - dim_region (region names)

    GRAIN:
    - One row per (year_month, region)

    MEASURES:
    - total_sales
    - total_profit
    - order_count
    - total_quantity
    """

    f = dlt.read(f"{GOLD_CAT}.fact_sales")
    reg = dlt.read(f"{GOLD_CAT}.dim_region")

    # Aggregate sales metrics
    a = f.groupBy("year_month", "region_key").agg(
        F.sum("sales_amount").alias("total_sales"),
        F.sum("profit").alias("total_profit"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("total_quantity"),
    )

    # Join with region dimension to get region name
    return a.join(reg, "region_key", "left")


# AGGREGATION: CUSTOMER RETURN SUMMARY

@dlt.table(
    name=f"{GOLD_CAT}.agg_customer_return_summary",
    comment="Customer-level return metrics for fraud detection"
)
def agg_customer_return_summary():
    """
    BUSINESS PURPOSE:
    - Identify customers with high return activity
    - Support fraud detection and policy decisions

    SOURCE:
    - fact_return
    - dim_region

    GRAIN:
    - One row per (customer_id, region)

    MEASURES:
    - return_count
    - total_refund_amount
    - avg_refund_amount
    """

    fr = dlt.read(f"{GOLD_CAT}.fact_return")
    reg = dlt.read(f"{GOLD_CAT}.dim_region")

    # Aggregate return metrics
    a = fr.groupBy("customer_id", "region_key").agg(
        F.count("*").alias("return_count"),
        F.sum("refund_amount").alias("total_refund_amount"),
        F.avg("refund_amount").alias("avg_refund_amount"),
    )

    # Join with region dimension
    return a.join(reg, "region_key", "left")


# AGGREGATION: VENDOR RETURN RATE

@dlt.table(
    name=f"{GOLD_CAT}.agg_vendor_return_rate",
    comment="Vendor performance — refund to sales ratio"
)
def agg_vendor_return_rate():
    """
    BUSINESS PURPOSE:
    - Evaluate vendor performance
    - Identify vendors with high return rates

    SOURCE:
    - fact_sales (sales data)
    - fact_return (refund data)
    - dim_vendor

    GRAIN:
    - One row per vendor

    MEASURES:
    - vendor_gross_sales
    - vendor_refunds
    - refund_to_sales_ratio
    """

    fs = dlt.read(f"{GOLD_CAT}.fact_sales")
    fr = dlt.read(f"{GOLD_CAT}.fact_return")
    v = dlt.read(f"{GOLD_CAT}.dim_vendor")

    # Total sales per vendor
    sales = fs.groupBy("vendor_id").agg(
        F.sum("sales_amount").alias("vendor_gross_sales")
    )

    # Total refunds per vendor
    refunds = fr.groupBy("vendor_id").agg(
        F.sum("refund_amount").alias("vendor_refunds")
    )

    # Combine sales and refunds
    out = sales.join(refunds, "vendor_id", "left").fillna(
        {"vendor_refunds": 0.0}
    )

    # Calculate return rate (important business metric)
    out = out.withColumn(
        "refund_to_sales_ratio",
        F.when(
            F.col("vendor_gross_sales") > 0,
            F.col("vendor_refunds") / F.col("vendor_gross_sales")
        ).otherwise(F.lit(None))
    )

    # Join with vendor dimension for context
    return out.join(v, "vendor_id", "left")


# AGGREGATION: PRODUCT PERFORMANCE

@dlt.table(
    name=f"{GOLD_CAT}.agg_product_region_monthly",
    comment="Product performance by region and month"
)
def agg_product_region_monthly():
    """
    BUSINESS PURPOSE:
    - Identify slow-moving and high-performing products
    - Support inventory planning and optimization

    SOURCE:
    - fact_sales
    - dim_region

    GRAIN:
    - One row per (product_id, region, month)

    MEASURES:
    - total_sales
    - total_quantity
    - total_profit
    - order_count
    """

    f = dlt.read(f"{GOLD_CAT}.fact_sales")
    reg = dlt.read(f"{GOLD_CAT}.dim_region")

    # Aggregate product-level metrics
    a = f.groupBy("year_month", "region_key", "product_id").agg(
        F.sum("sales_amount").alias("total_sales"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("profit").alias("total_profit"),
        F.countDistinct("order_id").alias("order_count"),
    )

    # Join with region dimension
    return a.join(reg, "region_key", "left")