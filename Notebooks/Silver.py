# GlobalMart — Silver layer (Delta Live Tables)

# Important: Table outputs use fully qualified names (`globalmart.silver.*`) so they never collide with `globalmart.bronze.*` if Bronze and Silver share one pipeline or the default schema is wrong. Do not add the Bronze notebook to this pipeline.

# What this does:
# 1. Harmonizes Bronze → canonical column names, types, and values (per entity).
# 2. Declares data-quality outcomes as split between `silver.<entity>` (pass) and `quarantine_<entity>` (fail with `failure_reasons`).
# 3. Materializes `quality_run_summary` — accepted vs rejected counts per entity for operations / Finance dashboards.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Target schema for Silver tables
SILVER_CAT = "globalmart.silver"

# DATE / TIMESTAMP PARSING
def _parse_ts(c):
    """
    Handles multiple timestamp formats observed across regions.
    
    BEFORE (Bronze):
    - Dates stored in inconsistent formats (MM/dd/yyyy, yyyy-MM-dd, etc.)
    
    AFTER (Silver):
    - All timestamps standardized into a single timestamp format
    - Uses coalesce to try multiple patterns safely
    """
    return F.coalesce(
        F.to_timestamp(c, "M/d/yyyy H:mm"),
        F.to_timestamp(c, "MM/dd/yyyy HH:mm"),
        F.to_timestamp(c, "M/d/yyyy HH:mm"),
        F.to_timestamp(c, "yyyy-MM-dd H:mm"),
        F.to_timestamp(c, "yyyy-MM-dd HH:mm"),
        F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(c, "yyyy-MM-dd H:mm:ss"),
    )


# SEGMENT NORMALIZATION
def _norm_segment(s):
    """
    Standardizes customer segment values.
    
    BEFORE:
    - Values like CORP, CONS, Cosumer (typo), etc.
    
    AFTER:
    - Standard values: Consumer, Corporate, Home Office
    """
    u = F.upper(F.trim(s))
    return (
        F.when(u.isin("CORP", "CORPORATE"), F.lit("Corporate"))
        .when(u.isin("CONS", "CONSUMER"), F.lit("Consumer"))
        .when(u == "COSUMER", F.lit("Consumer"))  # fixing typo
        .when(u.isin("HO", "HOME OFFICE"), F.lit("Home Office"))
        .otherwise(F.initcap(F.trim(s)))
    )

# REGION NORMALIZATION
def _norm_region(r):
    """
    Standardizes region values.
    
    BEFORE:
    - Region values inconsistent (E, East, W, West, etc.)
    
    AFTER:
    - All regions mapped to full names (East, West, North, South, Central)
    """
    x = F.upper(F.trim(r))
    return (
        F.when(x.isin("E", "EAST"), F.lit("East"))
        .when(x.isin("W", "WEST"), F.lit("West"))
        .when(x.isin("N", "NORTH"), F.lit("North"))
        .when(x.isin("S", "SOUTH"), F.lit("South"))
        .when(x.isin("C", "CENTRAL"), F.lit("Central"))
        .otherwise(F.initcap(F.trim(r)))
    )

# MONEY PARSING
def _parse_money(col_expr):
    """
    Cleans and converts monetary values.
    
    BEFORE:
    - Values contain '$' symbols and commas
    
    AFTER:
    - Clean numeric values (Double)
    """
    x = F.trim(col_expr.cast("string"))
    x = F.regexp_replace(x, "^\\$", "")
    x = F.regexp_replace(x, ",", "")
    return x.cast(DoubleType())

# DISCOUNT PARSING
def _parse_discount(col_expr):
    """
    Normalizes discount values.
    
    BEFORE:
    - Mixed formats: 0.2 (decimal) vs 20% (string)
    - Invalid values like '?' or empty
    
    AFTER:
    - Standard decimal format (0-1)
    """
    d = F.trim(col_expr.cast("string"))
    return (
        F.when(d.isNull() | (d == "") | (d == "?"), F.lit(None).cast(DoubleType()))
        .when(d.endswith("%"), F.regexp_replace(d, "%", "").cast(DoubleType()) / F.lit(100.0))
        .otherwise(d.cast(DoubleType()))
    )

# RETURN AMOUNT PARSING
def _parse_return_amount(col_expr):
    """
    Cleans refund amount values.
    
    BEFORE:
    - Values stored as strings with '$'
    
    AFTER:
    - Converted to numeric format
    """
    x = F.trim(col_expr.cast("string"))
    x = F.regexp_replace(x, "^\\$", "")
    x = F.regexp_replace(x, ",", "")
    return x.cast(DoubleType())

# RETURN DATE PARSING
def _parse_return_date(c):
    """
    Handles multiple date formats for returns.
    
    BEFORE:
    - Dates in different formats across JSON files
    
    AFTER:
    - Standard date format using safe parsing
    """
    d = F.trim(c.cast("string"))
    return F.coalesce(
        F.to_date(d, "yyyy-MM-dd"),
        F.to_date(d, "MM-dd-yyyy"),
        F.to_date(d, "dd-MM-yyyy"),
        F.to_date(d, "dd/MM/yyyy"),
        F.to_date(d, "MM/dd/yyyy"),
    )

# RETURN STATUS NORMALIZATION
def _norm_return_status(s):
    """
    Standardizes return status values.
    
    BEFORE:
    - Values like RJCTD, Rejected, Approved, etc.
    
    AFTER:
    - Standard values: Rejected, Approved, Pending
    """
    u = F.upper(F.trim(s))
    return (
        F.when(u.isin("RJCTD", "REJECTED"), F.lit("Rejected"))
        .when(u.isin("APPROVED"), F.lit("Approved"))
        .when(u.isin("PENDING"), F.lit("Pending"))
        .otherwise(F.initcap(F.trim(s)))
    )

# CUSTOMERS — BEFORE vs AFTER

# BEFORE (Bronze):
# - Multiple ID columns: customer_id, CustomerID, cust_id, customer_identifier
# - Email column inconsistent or missing across regions
# - Name column varies (customer_name vs full_name)
# - Segment values inconsistent (CORP, Cosumer, etc.)
# - Region values inconsistent (E, East, etc.)
# - City and State swapped in Region 4 files
# - No enforced data quality rules

# AFTER (Silver):
# - Unified customer_id using coalesce()
# - Standardized email, name, segment, and region values
# - Fixed city/state swap using source file metadata
# - Added email_missing_flag for optional missing emails
# - Applied data quality rules and captured failures
# - Clean data stored in Silver, invalid data routed to quarantine

# CUSTOMERS STAGING (Transformation Layer)

@dlt.view()
def customers_staging():
    # Read raw Bronze data
    df = spark.table("globalmart.bronze.customers")

    # Coalesce multiple regional ID columns into one standard column
    cid = F.coalesce(
        F.trim(F.col("customer_id")),
        F.trim(F.col("CustomerID")),
        F.trim(F.col("cust_id")),
        F.trim(F.col("customer_identifier")),
    )

    # Standardize email and name columns from different schemas (raw before placeholders)
    email_src = F.coalesce(F.col("customer_email"), F.col("email_address"))
    cname = F.coalesce(F.col("customer_name"), F.col("full_name"))

    # Detect Region 4 file using source path (used to fix swapped columns)
    path = F.col("_source_file_path")
    is_r4 = F.lower(path).contains("customers_4")

    # Fix structural issue: city and state swapped in Region 4
    city_c = F.when(is_r4, F.col("state")).otherwise(F.col("city"))
    state_c = F.when(is_r4, F.col("city")).otherwise(F.col("state"))

    reg = _norm_region(F.col("region"))
    seg_norm = _norm_segment(F.col("segment"))

    # Placeholder email for analytics joins (retain lineage via flag)
    email_missing = email_src.isNull() | (F.trim(email_src.cast("string")) == "")
    customer_email = F.when(email_missing, F.lit("missing_email")).otherwise(F.trim(email_src.cast("string")))

    # Apply transformations and derive quality flags
    out = (
        df.withColumn("_customer_id", cid)
        .withColumn("customer_email", customer_email)
        .withColumn("customer_name", cname)
        .withColumn("segment", seg_norm)
        .withColumn("city", city_c)
        .withColumn("state", state_c)
        .withColumn("region", reg)
        .withColumn("email_missing_flag", email_missing)

        # Preserve lineage from Bronze
        .withColumn("bronze_ingest_ts", F.col("_ingest_timestamp"))

        # Build failure reasons for quality rule violations
        .withColumn(
            "failure_reasons",
            F.concat_ws(
                ";",
                # Critical: customer_id must exist
                F.when(
                    F.col("_customer_id").isNull() | (F.trim(F.col("_customer_id")) == ""),
                    F.lit("null_customer_id"),
                ),
                # Segment must be valid 
                F.when(
                    F.col("segment").isNull(),
                    F.lit("Not_valid_segment_or_null"),
                ),
                # Country validation (business rule)
                F.when(
                    F.col("country").isNotNull()
                    & (F.trim(F.col("country").cast("string")) != "United States"),
                    F.lit("non_us_country"),
                ),
            ),
        )
    )

    # Finalize standardized column
    out = out.withColumn("customer_id", F.col("_customer_id")).drop("_customer_id")

    return out


# CLEAN DATA → SILVER TABLE
@dlt.table(name=f"{SILVER_CAT}.customers")
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
def customers():
    st = dlt.read("customers_staging")

    # Keep only valid records (no failure reasons)
    clean = st.filter(
        (F.col("failure_reasons") == "") | F.col("failure_reasons").isNull()
    ).drop("failure_reasons")

    # Select finalized Silver schema
    return clean.select(
        "customer_id",
        "customer_email",
        "customer_name",
        "segment",
        "country",
        "city",
        "state",
        "postal_code",
        "region",
        "email_missing_flag",
        "_source_file_path",
        "bronze_ingest_ts",
        F.current_timestamp().alias("silver_load_ts"),
    )

# FAILED DATA → QUARANTINE TABLE

@dlt.table(
    name=f"{SILVER_CAT}.quarantine_customers",
    comment="Rejected customer rows — contains failure reasons for investigation",
)
def quarantine_customers():
    st = dlt.read("customers_staging")

    # Capture all failed records with reasons (no data loss)
    bad = st.filter(F.col("failure_reasons").isNotNull() & (F.col("failure_reasons") != ""))
    return bad.withColumn("silver_evaluated_at", F.current_timestamp())


# ORDERS — BEFORE vs AFTER

# BEFORE (Bronze):
# - Date columns in multiple formats across regions (MM/dd/yyyy, yyyy-MM-dd, etc.)
# - Some missing fields (e.g., estimated delivery date in some files)
# - Ship mode values inconsistent (First Class, 1st Class, Std Class, etc.)
# - No enforced data quality rules
# - Potential invalid or missing keys (order_id, customer_id, vendor_id)

# AFTER (Silver):
# - All date fields parsed into consistent timestamp format
# - Ship mode standardized into defined categories
# - Missing values handled gracefully (NULL where allowed)
# - Data quality rules enforced
# - Clean records stored in Silver table
# - Invalid records routed to quarantine with failure reasons

# ORDERS STAGING (Transformation Layer)

@dlt.view()
def orders_staging():
    # Read raw Bronze orders data
    df = spark.table("globalmart.bronze.orders")

    # Parse multiple date formats into standardized timestamps
    purchase_ts = _parse_ts(F.col("order_purchase_date"))
    approved_ts_raw = _parse_ts(F.col("order_approved_at"))
    carrier_ts_raw = _parse_ts(F.col("order_delivered_carrier_date"))
    cust_ts_raw = _parse_ts(F.col("order_delivered_customer_date"))
    est_ts = _parse_ts(F.col("order_estimated_delivery_date"))

    # Null handling: backfill approval from purchase; ETA may remain null (acceptable)
    auto_approved_flag = approved_ts_raw.isNull()
    order_approved_ts = F.coalesce(approved_ts_raw, purchase_ts)

    carrier_delay_flag = carrier_ts_raw.isNull()
    customer_delay_flag = cust_ts_raw.isNull()
    carrier_fulfillment_status = F.when(carrier_ts_raw.isNull(), F.lit("Not picked")).otherwise(
        F.lit("Picked up / in transit")
    )
    customer_delivery_status = F.when(cust_ts_raw.isNull(), F.lit("Not delivered")).otherwise(F.lit("Delivered"))

    # Normalize ship mode values across regions
    ship = F.upper(F.trim(F.col("ship_mode")))
    ship_c = (
        F.when(ship.contains("1ST") | ship.contains("FIRST"), F.lit("First Class"))
        .when(ship.contains("2ND") | ship.contains("SECOND"), F.lit("Second Class"))
        .when(ship.contains("STD") | ship.contains("STANDARD"), F.lit("Standard Class"))
        .when(ship.contains("SAME"), F.lit("Same Day"))
        .when(ship == "", F.lit("Unknown"))
        .otherwise(F.initcap(F.trim(F.col("ship_mode"))))
    )

    # Apply transformations and define quality rules
    out = (
        df.withColumn("order_purchase_ts", purchase_ts)
        .withColumn("order_approved_ts", order_approved_ts)
        .withColumn("order_delivered_carrier_ts", carrier_ts_raw)
        .withColumn("order_delivered_customer_ts", cust_ts_raw)
        .withColumn("order_estimated_delivery_ts", est_ts)
        .withColumn("ship_mode", ship_c)
        .withColumn("auto_approved_flag", auto_approved_flag)
        .withColumn("carrier_delay_flag", carrier_delay_flag)
        .withColumn("customer_delay_flag", customer_delay_flag)
        .withColumn("carrier_fulfillment_status", carrier_fulfillment_status)
        .withColumn("customer_delivery_status", customer_delivery_status)

        # Preserve Bronze lineage
        .withColumn("bronze_ingest_ts", F.col("_ingest_timestamp"))

        # Build failure reasons for invalid records
        .withColumn(
            "failure_reasons",
            F.concat_ws(
                ";",
                # order_id must exist
                F.when(
                    F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""),
                    F.lit("null_order_id")
                ),
                # customer_id must exist
                F.when(
                    F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == ""),
                    F.lit("null_customer_id")
                ),
                # purchase date must be valid
                F.when(
                    F.col("order_purchase_ts").isNull(),
                    F.lit("unparseable_order_purchase_date")
                ),
                # ship_mode must exist
                F.when(
                    F.col("ship_mode").isNull(),
                    F.lit("null_ship_mode")
                ),
                # vendor_id must exist
                F.when(
                    F.col("vendor_id").isNull() | (F.trim(F.col("vendor_id").cast("string")) == ""),
                    F.lit("null_vendor_id")
                ),
                # vendor_id must follow expected format (VENXX)
                F.when(
                    F.col("vendor_id").isNotNull()
                    & ~F.trim(F.col("vendor_id").cast("string")).rlike("^VEN[0-9]{2}$"),
                    F.lit("invalid_vendor_id")
                ),
            ),
        )
    )

    return out

# CLEAN DATA → SILVER TABLE
@dlt.table(name=f"{SILVER_CAT}.orders")
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_purchase_date", "order_purchase_date IS NOT NULL")
@dlt.expect("valid_vendor_id", "vendor_id RLIKE '^VEN[0-9]{2}$'")
def orders():
    st = dlt.read("orders_staging")

    # Keep only valid records
    ok = st.filter(
        (F.col("failure_reasons") == "") | F.col("failure_reasons").isNull()
    ).drop("failure_reasons")

    # Final standardized schema for Silver
    return ok.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("vendor_id"),
        F.col("ship_mode"),
        F.col("order_status"),
        F.col("order_purchase_ts").alias("order_purchase_date"),
        F.col("order_approved_ts").alias("order_approved_at"),
        F.col("order_delivered_carrier_ts").alias("order_delivered_carrier_date"),
        F.col("order_delivered_customer_ts").alias("order_delivered_customer_date"),
        F.col("order_estimated_delivery_ts").alias("order_estimated_delivery_date"),
        F.col("auto_approved_flag"),
        F.col("carrier_delay_flag"),
        F.col("customer_delay_flag"),
        F.col("carrier_fulfillment_status"),
        F.col("customer_delivery_status"),
        F.col("_source_file_path"),
        F.col("bronze_ingest_ts"),
        F.current_timestamp().alias("silver_load_ts"),
    )

# FAILED DATA → QUARANTINE TABLE

@dlt.table(name=f"{SILVER_CAT}.quarantine_orders")
def quarantine_orders():
    st = dlt.read("orders_staging")

    # Capture all failed records with reasons (no silent data loss)
    bad = st.filter(F.col("failure_reasons").isNotNull() & (F.col("failure_reasons") != ""))

    return bad.withColumn("silver_evaluated_at", F.current_timestamp())


# TRANSACTIONS — BEFORE vs AFTER

# BEFORE (Bronze):
# - Product_id variations across regions
# - Sales values contain '$' symbol or text formats
# - Discount stored as decimal (0.2) or percentage string (20%)
# - Quantity contains invalid values like '?' or empty strings
# - Missing profit column in some files
# - No enforced data quality rules

# AFTER (Silver):
# - Standardized column names (order_id, product_id)
# - Sales cleaned and converted to numeric values
# - Discount normalized into decimal format (0–1)
# - Quantity cleaned and safely cast to numeric
# - Profit handled with NULL + flag if missing
# - Data quality rules applied
# - Clean records stored in Silver table
# - Invalid records routed to quarantine with failure reasons

# TRANSACTIONS STAGING (Transformation Layer)

@dlt.view()
def transactions_staging():
    # Read raw Bronze transactions data
    df = spark.table("globalmart.bronze.transactions")

    # Standardize order_id and product_id across schema variations
    oid = F.coalesce(F.col("Order_id"), F.col("order_id"), F.col("Order_ID"))
    pid = F.coalesce(F.col("Product_id"), F.col("product_id"), F.col("Product_ID"))

    # Clean sales amount (remove '$', cast to numeric)
    sales_amt = _parse_money(F.coalesce(F.col("Sales"), F.col("sales")))

    # Clean quantity (handle '?', empty values → NULL)
    qty_raw = F.trim(F.col("Quantity").cast("string"))
    qty = F.when(
        (qty_raw == "") | (qty_raw == "?"),
        F.lit(None).cast(DoubleType())
    ).otherwise(qty_raw.cast(DoubleType()))

    # Normalize discount (handle % and decimal formats)
    disc = _parse_discount(F.col("discount"))

    # Handle missing profit column (allowed but flagged)
    prof = F.when(
        F.col("profit").isNull(),
        F.lit(None).cast(DoubleType())
    ).otherwise(F.col("profit").cast(DoubleType()))

    # Apply transformations and quality rules
    out = (
        df.withColumn("order_id", oid)
        .withColumn("product_id", pid)
        .withColumn("sales_amount", sales_amt)
        .withColumn("quantity", qty)
        .withColumn("discount", disc)
        .withColumn("profit", prof)

        # Flag missing profit (not a failure, but tracked)
        .withColumn(
            "profit_missing_flag",
            F.col("profit").isNull(),
        )

        # Preserve Bronze lineage
        .withColumn("bronze_ingest_ts", F.col("_ingest_timestamp"))

        # Define quality rules and failure reasons
        .withColumn(
            "failure_reasons",
            F.concat_ws(
                ";",
                # order_id must exist
                F.when(
                    F.col("order_id").isNull() | (F.trim(F.col("order_id").cast("string")) == ""),
                    F.lit("null_order_id")
                ),
                # product_id must exist
                F.when(
                    F.col("product_id").isNull() | (F.trim(F.col("product_id").cast("string")) == ""),
                    F.lit("null_product_id")
                ),
                # sales must be valid numeric
                F.when(
                    F.col("sales_amount").isNull(),
                    F.lit("unparseable_sales")
                ),
                # quantity must exist
                F.when(
                    F.col("quantity").isNull(),
                    F.lit("null_quantity")
                ),
                # payment_type must be exist
                F.when(
                    F.col("payment_type").isNull(),
                    F.lit("null_payment_type")
                ),
                # sales should not be negative
                F.when(
                    F.col("sales_amount") < 0,
                    F.lit("negative_sales")
                ),
                # quantity must be positive if present
                F.when(
                    F.col("quantity").isNull() & (F.col("quantity") <= 0),
                    F.lit("non_positive_quantity_or_null")
                ),
            ),
        )
    )

    return out

# CLEAN DATA → SILVER TABLE
@dlt.table(name=f"{SILVER_CAT}.transactions")
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_product_id", "product_id IS NOT NULL")
def transactions():

    st = dlt.read("transactions_staging")

    # Keep only valid records
    ok = st.filter(
        (F.col("failure_reasons") == "") | F.col("failure_reasons").isNull()
    ).drop("failure_reasons")

    # Final Silver schema (clean + standardized)
    return ok.select(
        "order_id",
        "product_id",
        "sales_amount",
        "quantity",
        "discount",
        "profit",
        "profit_missing_flag",

        # Default missing payment_type to 'unknown'
        F.coalesce(F.col("payment_type"), F.lit("unknown")).alias("payment_type"),

        # Ensure consistent datatype
        F.col("payment_installments").cast("string"),

        "_source_file_path",
        "bronze_ingest_ts",
        F.current_timestamp().alias("silver_load_ts"),
    )

# FAILED DATA → QUARANTINE TABLE

@dlt.table(name=f"{SILVER_CAT}.quarantine_transactions")
def quarantine_transactions():
    st = dlt.read("transactions_staging")

    # Capture all failed records with reasons (ensures no data loss)
    bad = st.filter(F.col("failure_reasons").isNotNull() & (F.col("failure_reasons") != ""))

    return bad.withColumn("silver_evaluated_at", F.current_timestamp())

# RETURNS — BEFORE vs AFTER

# BEFORE (Bronze):
# - Different JSON schemas across regions (OrderId vs order_id)
# - Return reason column varies (reason vs return_reason)
# - Refund amount stored as string with '$'
# - Date formats inconsistent across files
# - Return status inconsistent (RJCTD, Rejected, etc.)
# - No enforced data quality rules

# AFTER (Silver):
# - Unified column names using coalesce()
# - Refund amount cleaned and converted to numeric
# - Return date parsed into standard date format
# - Return status normalized into standard categories
# - Data quality rules applied
# - Clean records stored in Silver table
# - Invalid records routed to quarantine with failure reasons

# RETURNS STAGING (Transformation Layer)

@dlt.view()
def returns_staging():
    # Read raw Bronze returns data
    df = spark.table("globalmart.bronze.returns")

    # Standardize schema across different JSON structures
    oid = F.coalesce(F.col("OrderId"), F.col("order_id"))
    reason = F.coalesce(F.col("reason"), F.col("return_reason"))

    # Clean refund amount (remove '$', cast to numeric)
    amt = _parse_return_amount(F.coalesce(F.col("amount"), F.col("refund_amount")))

    # Parse multiple date formats into standard date
    rdt = _parse_return_date(F.coalesce(F.col("date_of_return"), F.col("return_date")))

    # Normalize return status values
    st = _norm_return_status(F.coalesce(F.col("status"), F.col("return_status")))

    # Apply transformations and define quality rules
    out = (
        df.withColumn("order_id", oid)
        .withColumn("return_reason", reason)
        .withColumn("refund_amount", amt)
        .withColumn("return_date", rdt)
        .withColumn("return_status", st)

        # Preserve Bronze lineage
        .withColumn("bronze_ingest_ts", F.col("_ingest_timestamp"))

        # Define failure conditions
        .withColumn(
            "failure_reasons",
            F.concat_ws(
                ";",
                # order_id must exist
                F.when(
                    F.col("order_id").isNull() | (F.trim(F.col("order_id").cast("string")) == "") | (F.col("order_id") == "?"),
                    F.lit("null_order_id_or_not_valid")
                ),
                # return date must be valid
                F.when(
                    F.col("return_date").isNull(),
                    F.lit("unparseable_return_date")
                ),
                # refund amount must be valid
                F.when(
                    F.col("refund_amount").isNull(),
                    F.lit("null_refund_amount_or_not_valid")
                ),
                # return reason must exist
                F.when(
                    F.col("return_reason").isNull() | (F.col("return_reason") == "?"),
                    F.lit("null_return_reason_or_not_valid")
                ),
                # return status must be standardized
                F.when(
                    ~F.col("return_status").isin("Rejected", "Approved", "Pending"),
                    F.lit("invalid_return_status")
                ),
            ),
        )
    )

    return out

# CLEAN DATA → SILVER TABLE
@dlt.table(name=f"{SILVER_CAT}.returns")
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_refund_amount", "refund_amount IS NOT NULL")
@dlt.expect("valid_return_date", "return_date IS NOT NULL")
def returns():
    st = dlt.read("returns_staging")

    # Keep only valid records
    ok = st.filter(
        (F.col("failure_reasons") == "") | F.col("failure_reasons").isNull()
    ).drop("failure_reasons")

    # Final Silver schema
    return ok.select(
        "order_id",
        "return_reason",
        "refund_amount",
        "return_date",
        "return_status",
        "_source_file_path",
        "bronze_ingest_ts",
        F.current_timestamp().alias("silver_load_ts"),
    )

# FAILED DATA → QUARANTINE TABLE

@dlt.table(name=f"{SILVER_CAT}.quarantine_returns")
def quarantine_returns():
    st = dlt.read("returns_staging")

    # Capture failed records for investigation (no silent drop)
    bad = st.filter(F.col("failure_reasons").isNotNull() & (F.col("failure_reasons") != ""))
    return bad.withColumn("silver_evaluated_at", F.current_timestamp())


# PRODUCTS — BEFORE vs AFTER

# BEFORE (Bronze):
# - Mostly clean dataset but with many optional NULL fields
# - No enforced data quality rules

# AFTER (Silver):
# - Product ID and name validated
# - Data quality rules applied
# - Clean records stored in Silver table
# - Invalid records routed to quarantine with failure reasons

# PRODUCTS STAGING (Transformation Layer)

@dlt.view()
def products_staging():
    # Read raw Bronze products data
    df = spark.table("globalmart.bronze.products")

    out = (
        # Preserve Bronze lineage
        df.withColumn("bronze_ingest_ts", F.col("_ingest_timestamp"))

        # Define quality rules
        .withColumn(
            "failure_reasons",
            F.concat_ws(
                ";",
                # product_id must exist
                F.when(
                    F.col("product_id").isNull() | (F.trim(F.col("product_id").cast("string")) == ""),
                    F.lit("null_product_id"),
                ),
                # product_name must exist
                F.when(
                    F.col("product_name").isNull() | (F.trim(F.col("product_name").cast("string")) == ""),
                    F.lit("empty_product_name"),
                ),
            ),
        )
        
        # Droping the rescued data column
        .drop("_rescued_data")
    )
    return out

# CLEAN DATA → SILVER TABLE
@dlt.table(name=f"{SILVER_CAT}.products")
@dlt.expect("valid_product_id", "product_id IS NOT NULL")
def products():
    st = dlt.read("products_staging")

    # Keep only valid records
    ok = st.filter(
        (F.col("failure_reasons") == "") | F.col("failure_reasons").isNull()
    ).drop("failure_reasons")

    return ok.withColumn("silver_load_ts", F.current_timestamp())

# FAILED DATA → QUARANTINE TABLE

@dlt.table(name=f"{SILVER_CAT}.quarantine_products")
def quarantine_products():
    st = dlt.read("products_staging")

    # Capture failed records (no silent data loss)
    bad = st.filter(F.col("failure_reasons").isNotNull() & (F.col("failure_reasons") != ""))

    return bad.withColumn("silver_evaluated_at", F.current_timestamp())

# VENDORS — BEFORE vs AFTER

# BEFORE (Bronze):
# - Small reference dataset
# - Minor issues like null or untrimmed values
# - No enforced data quality rules

# AFTER (Silver):
# - Cleaned and trimmed vendor_id and vendor_name
# - Enforced primary key and required fields
# - Data quality rules applied
# - Clean records stored in Silver table
# - Invalid records routed to quarantine

# VENDORS STAGING (Transformation Layer)

@dlt.view()
def vendors_staging():
    # Read raw Bronze vendors data
    df = spark.table("globalmart.bronze.vendors")

    out = (
        df.withColumn("bronze_ingest_ts", F.col("_ingest_timestamp"))

        # Define quality rules
        .withColumn(
            "failure_reasons",
            F.concat_ws(
                ";",
                # vendor_id must exist
                F.when(
                    F.col("vendor_id").isNull() | (F.trim(F.col("vendor_id").cast("string")) == ""),
                    F.lit("null_vendor_id"),
                ),
                # vendor_name must exist
                F.when(
                    F.col("vendor_name").isNull() | (F.trim(F.col("vendor_name").cast("string")) == ""),
                    F.lit("null_vendor_name"),
                ),
            ),
        )
    )
    return out

# CLEAN DATA → SILVER TABLE
@dlt.table(name=f"{SILVER_CAT}.vendors")
@dlt.expect("valid_vendor_id", "vendor_id IS NOT NULL")
def vendors():
    st = dlt.read("vendors_staging")

    # Keep only valid records
    ok = st.filter(
        (F.col("failure_reasons") == "") | F.col("failure_reasons").isNull()
    ).drop("failure_reasons")

    return ok.select(
        # Trim values for consistency
        F.trim(F.col("vendor_id")).alias("vendor_id"),
        F.trim(F.col("vendor_name")).alias("vendor_name"),
        "_source_file_path",
        "bronze_ingest_ts",
        F.current_timestamp().alias("silver_load_ts"),
    )

# FAILED DATA → QUARANTINE TABLE

@dlt.table(name=f"{SILVER_CAT}.quarantine_vendors")
def quarantine_vendors():
    st = dlt.read("vendors_staging")

    # Capture failed records (ensures no data loss)
    bad = st.filter(F.col("failure_reasons").isNotNull() & (F.col("failure_reasons") != ""))

    return bad.withColumn("silver_evaluated_at", F.current_timestamp())


# QUALITY RUN SUMMARY — BEFORE vs AFTER

# BEFORE:
# - No visibility into how many records passed or failed quality rules
# - No centralized tracking of data quality issues across entities
# - Business teams (Finance) cannot easily monitor data quality trends

# AFTER:
# - Centralized table showing accepted vs rejected record counts per entity
# - Enables monitoring of data quality for each pipeline run
# - Helps Finance and operations quickly identify issues (e.g., spikes in rejected records)
# - Fully queryable and can be used in dashboards or alerts

# QUALITY RUN SUMMARY TABLE

@dlt.table(name=f"{SILVER_CAT}.quality_run_summary")
def quality_run_summary():

    # Define all entities and their corresponding quarantine tables
    pairs = [
        ("customers", "quarantine_customers"),
        ("orders", "quarantine_orders"),
        ("transactions", "quarantine_transactions"),
        ("returns", "quarantine_returns"),
        ("products", "quarantine_products"),
        ("vendors", "quarantine_vendors"),
    ]

    acc_df = None
    rej_df = None

    # Loop through each entity to calculate accepted and rejected counts
    for ent, qtbl in pairs:

        # Count accepted (clean) records from Silver table
        a = dlt.read(f"{SILVER_CAT}.{ent}").agg(
            F.lit(ent).alias("entity"),
            F.count("*").alias("accepted_count"),
        )

        # Count rejected (failed) records from quarantine table
        r = dlt.read(f"{SILVER_CAT}.{qtbl}").agg(
            F.lit(ent).alias("entity"),
            F.count("*").alias("rejected_count"),
        )

        # Combine results across all entities
        acc_df = a if acc_df is None else acc_df.unionByName(a)
        rej_df = r if rej_df is None else rej_df.unionByName(r)

    # Join accepted and rejected counts into a single summary table
    return acc_df.join(rej_df, on="entity", how="outer").withColumn("evaluated_at", F.current_timestamp())