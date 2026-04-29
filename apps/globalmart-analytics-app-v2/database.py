import traceback
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

# =============================================
# CONFIGURATION
# =============================================
cfg = Config()  # Auto-detects credentials from Databricks App service principal

WAREHOUSE_HTTP_PATH  = "/sql/1.0/warehouses/182e193ca24a9290"
CATALOG              = "globalmart_lb"   # Unity Catalog
SCHEMA               = "gold"


def _get_server_hostname():
    """Extract clean hostname from SDK Config (strips https:// prefix)."""
    hostname = cfg.host
    if hostname.startswith("https://"):
        hostname = hostname.replace("https://", "")
    elif hostname.startswith("http://"):
        hostname = hostname.replace("http://", "")
    return hostname


def _make_connection():
    return sql.connect(
        server_hostname=_get_server_hostname(),
        http_path=WAREHOUSE_HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
        _socket_timeout=300,
    )


@st.cache_resource(show_spinner=False, ttl=1800)
def get_sql_connection():
    """Cached SQL connection — reused across reruns, auto-refreshed every 30 min."""
    return _make_connection()


def run_query(query: str) -> pd.DataFrame:
    """Execute SQL; auto-reconnects once if the connection is stale."""
    try:
        conn = get_sql_connection()
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows    = cursor.fetchall()
            columns = [d[0] for d in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    except Exception as first_err:
        print(f"[run_query] First attempt failed ({type(first_err).__name__}): {first_err}")
        print(traceback.format_exc())
        try:
            get_sql_connection.clear()
            conn = get_sql_connection()
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows    = cursor.fetchall()
                columns = [d[0] for d in cursor.description]
            return pd.DataFrame(rows, columns=columns)
        except Exception as second_err:
            print(f"[run_query] Retry also failed ({type(second_err).__name__}): {second_err}")
            print(traceback.format_exc())
            raise second_err


def _t(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"


# =============================================
# CACHED DATA LOADING  (runs ONCE, cached 30 min — reduces warehouse cost)
# =============================================
@st.cache_data(show_spinner="Loading sales data…", ttl=1800)
def _load_fact_sales() -> pd.DataFrame:
    """Load full fact_sales joined with dim_region + dim_date (only needed cols)."""
    return run_query(f"""
        SELECT fs.order_id, fs.customer_id, fs.customer_segment,
               fs.product_id, fs.vendor_id, fs.region_key,
               fs.sales_amount, fs.quantity, fs.profit,
               fs.year_month,
               r.region_name
        FROM   {_t('fact_sales')}  fs
        JOIN   {_t('dim_region')}  r ON fs.region_key = r.region_key
    """)


@st.cache_data(show_spinner="Loading return data…", ttl=1800)
def _load_fact_return() -> pd.DataFrame:
    """Load full fact_return joined with dim_region."""
    return run_query(f"""
        SELECT fr.return_key, fr.order_id, fr.return_reason,
               fr.refund_amount, fr.return_date_key, fr.return_status,
               fr.vendor_id, fr.customer_id, fr.region_key,
               r.region_name
        FROM   {_t('fact_return')} fr
        JOIN   {_t('dim_region')}  r ON fr.region_key = r.region_key
    """)


@st.cache_data(show_spinner="Loading customer data…", ttl=1800)
def _load_dim_customer() -> pd.DataFrame:
    return run_query(f"""
        SELECT customer_id, customer_segment, region_name, region_key
        FROM   {_t('dim_customer')}
    """)


@st.cache_data(show_spinner="Loading return summary…", ttl=1800)
def _load_agg_return_summary() -> pd.DataFrame:
    return run_query(f"""
        SELECT customer_id, region_key, region_name,
               return_count, total_refund_amount, avg_refund_amount
        FROM   {_t('agg_customer_return_summary')}
    """)


@st.cache_data(show_spinner="Loading vendor data…", ttl=1800)
def _load_dim_vendor() -> pd.DataFrame:
    return run_query(f"""
        SELECT vendor_id, vendor_name
        FROM   {_t('dim_vendor')}
    """)


@st.cache_data(show_spinner="Loading date data…", ttl=1800)
def _load_dim_date() -> pd.DataFrame:
    return run_query(f"""
        SELECT date_key, year_month
        FROM   {_t('dim_date')}
    """)


# =============================================
# KPI COMPUTATION  (pure pandas — instant on filter change)
# =============================================
def fetch_kpi_data(regions=None, segments=None):
    """Compute all KPIs from cached DataFrames — no SQL on filter change."""

    query_errors = []

    def handle_err(label, e):
        tb = traceback.format_exc()
        full_msg = f"{type(e).__name__}: {e}"
        print(f"COMPUTE ERROR [{label}]: {full_msg}\n{tb}")
        query_errors.append(f"{label}: {full_msg}")

    # ── Load cached base data (only hits SQL on first call / TTL expiry) ──────
    try:
        df_sales    = _load_fact_sales()
        df_returns  = _load_fact_return()
        df_cust     = _load_dim_customer()
        df_agg      = _load_agg_return_summary()
        df_vendor   = _load_dim_vendor()
    except Exception as e:
        handle_err("Data load", e)
        # Return empty defaults so app doesn't crash
        return _empty_result(query_errors)

    # ── Apply filters in pandas ───────────────────────────────────────────────
    if regions and 0 < len(regions) < 5:
        df_sales   = df_sales[df_sales["region_name"].isin(regions)]
        df_returns = df_returns[df_returns["region_name"].isin(regions)]
        df_cust    = df_cust[df_cust["region_name"].isin(regions)]
        df_agg     = df_agg[df_agg["region_name"].isin(regions)]

    if segments and 0 < len(segments) < 3:
        df_sales = df_sales[df_sales["customer_segment"].isin(segments)]
        df_cust  = df_cust[df_cust["customer_segment"].isin(segments)]

    # ═════════════════════════════════════════════════════════════
    # SALES KPIs (from fact_sales — Overview tab)
    # ═════════════════════════════════════════════════════════════
    total_revenue = 0
    total_profit  = 0
    total_orders  = 0
    total_quantity = 0

    try:
        total_revenue  = df_sales["sales_amount"].sum() or 0
        total_profit   = df_sales["profit"].sum() or 0
        total_orders   = df_sales["order_id"].nunique()
        total_quantity  = int(df_sales["quantity"].sum() or 0)
    except Exception as e:
        handle_err("Sales KPIs", e)

    # ── Revenue & Profit by Region ────────────────────────────────────────────
    revenue_by_region = pd.DataFrame({"region_name": ["N/A"], "revenue": [0], "profit": [0]})
    try:
        if not df_sales.empty:
            rbr = df_sales.groupby("region_name", as_index=False).agg(
                revenue=("sales_amount", "sum"),
                profit=("profit", "sum")
            ).sort_values("revenue", ascending=False)
            rbr["revenue"] = rbr["revenue"].round(2)
            rbr["profit"]  = rbr["profit"].fillna(0).round(2)
            if not rbr.empty:
                revenue_by_region = rbr
    except Exception as e:
        handle_err("Region revenue", e)

    # ── Customer Segments ─────────────────────────────────────────────────────
    customer_segments = pd.DataFrame({"customer_segment": ["N/A"], "cnt": [0]})
    try:
        if not df_cust.empty:
            cs = df_cust.groupby("customer_segment", as_index=False).size()
            cs.columns = ["customer_segment", "cnt"]
            if not cs.empty:
                customer_segments = cs
    except Exception as e:
        handle_err("Segments", e)

    # ── Monthly Sales Trend ───────────────────────────────────────────────────
    monthly_trend = pd.DataFrame(columns=["year_month", "total_sales", "total_profit", "order_count"])
    try:
        if not df_sales.empty:
            mt = df_sales.groupby("year_month", as_index=False).agg(
                total_sales=("sales_amount", "sum"),
                total_profit=("profit", "sum"),
                order_count=("order_id", "nunique")
            ).sort_values("year_month")
            mt["total_sales"]  = mt["total_sales"].round(2)
            mt["total_profit"] = mt["total_profit"].fillna(0).round(2)
            if not mt.empty:
                monthly_trend = mt
    except Exception as e:
        handle_err("Monthly trend", e)

    # ═════════════════════════════════════════════════════════════
    # RETURN KPIs (from fact_return — Returns tab)
    # ═════════════════════════════════════════════════════════════

    # ── Top Return Reasons ────────────────────────────────────────────────────
    top_products = pd.DataFrame({"product_name": ["N/A"], "total": [0], "units_sold": [0]})
    try:
        if not df_returns.empty:
            rr = df_returns.groupby("return_reason", as_index=False).agg(
                total=("return_key", "count"),
                units_sold=("refund_amount", "sum")
            ).sort_values("total", ascending=False).head(10)
            rr["units_sold"] = rr["units_sold"].round(2)
            rr = rr.rename(columns={"return_reason": "product_name"})
            if not rr.empty:
                top_products = rr
    except Exception as e:
        handle_err("Top products", e)

    # ── Return Status Breakdown ───────────────────────────────────────────────
    category_perf = pd.DataFrame({"category": ["N/A"], "revenue": [0], "profit": [0], "orders": [0]})
    try:
        if not df_returns.empty:
            cp = df_returns.groupby("return_status", as_index=False).agg(
                revenue=("refund_amount", "sum"),
                profit=("return_key", "count"),
                orders=("order_id", "nunique")
            ).sort_values("revenue", ascending=False)
            cp["revenue"] = cp["revenue"].round(2)
            cp = cp.rename(columns={"return_status": "category"})
            if not cp.empty:
                category_perf = cp
    except Exception as e:
        handle_err("Category perf", e)

    # ── High-Risk Customers (≥5 returns) ──────────────────────────────────────
    high_risk_customers = 0
    try:
        high_risk_customers = int((df_agg["return_count"] >= 5).sum())
    except Exception as e:
        handle_err("High-risk", e)

    # ── Pending Returns ───────────────────────────────────────────────────────
    slow_moving_products = 0
    try:
        slow_moving_products = int((df_returns["return_status"] == "Pending").sum())
    except Exception as e:
        handle_err("Slow-moving", e)

    # ── Top Return Customers ──────────────────────────────────────────────────
    top_returners = pd.DataFrame()
    try:
        if not df_agg.empty:
            tr = df_agg.nlargest(10, "return_count")[
                ["customer_id", "region_name", "return_count",
                 "total_refund_amount", "avg_refund_amount"]
            ].copy()
            tr["total_refund_amount"] = tr["total_refund_amount"].round(2)
            tr["avg_refund_amount"]   = tr["avg_refund_amount"].round(2)
            top_returners = tr
    except Exception as e:
        handle_err("Top returners", e)

    # ═════════════════════════════════════════════════════════════
    # VENDOR PERFORMANCE (from fact_sales — Vendors tab)
    # ═════════════════════════════════════════════════════════════
    vendor_perf = pd.DataFrame({"vendor_name": ["N/A"], "vendor_gross_sales": [0], "refund_pct": [0]})
    try:
        if not df_sales.empty:
            vp = df_sales.merge(df_vendor, on="vendor_id", how="left")
            vp = vp.groupby("vendor_name", as_index=False).agg(
                vendor_gross_sales=("sales_amount", "sum"),
                total_profit=("profit", "sum")
            ).sort_values("vendor_gross_sales", ascending=False).head(10)
            vp["vendor_gross_sales"] = vp["vendor_gross_sales"].round(2)
            vp["refund_pct"] = (
                vp["total_profit"].fillna(0) / vp["vendor_gross_sales"].replace(0, pd.NA) * 100
            ).round(1).fillna(0)
            vp = vp.drop(columns=["total_profit"])
            if not vp.empty:
                vendor_perf = vp
    except Exception as e:
        handle_err("Vendor perf", e)

    # ── Derived summary metrics ───────────────────────────────────────────────
    if not revenue_by_region.empty and revenue_by_region["revenue"].sum() > 0:
        top_region  = revenue_by_region.loc[revenue_by_region["revenue"].idxmax(), "region_name"]
        weak_region = revenue_by_region.loc[revenue_by_region["revenue"].idxmin(), "region_name"]
    else:
        top_region  = "N/A"
        weak_region = "N/A"

    profit_margin_pct = round(
        (total_profit / total_revenue * 100) if total_revenue > 0 else 0, 1
    )

    return {
        "total_revenue":        total_revenue,
        "total_profit":         total_profit,
        "total_orders":         total_orders,
        "total_quantity":       total_quantity,
        "profit_margin_pct":    profit_margin_pct,
        "revenue_by_region":    revenue_by_region,
        "monthly_trend":        monthly_trend,
        "category_perf":        category_perf,
        "top_region":           top_region,
        "weak_region":          weak_region,
        "customer_segments":    customer_segments,
        "top_products":         top_products,
        "vendor_perf":          vendor_perf,
        "top_returners":        top_returners,
        "high_risk_customers":  high_risk_customers,
        "slow_moving_products": slow_moving_products,
        "query_errors":         query_errors,
    }


def _empty_result(query_errors):
    """Return empty defaults when data load fails entirely."""
    return {
        "total_revenue": 0, "total_profit": 0, "total_orders": 0,
        "total_quantity": 0, "profit_margin_pct": 0,
        "revenue_by_region": pd.DataFrame({"region_name": ["N/A"], "revenue": [0], "profit": [0]}),
        "monthly_trend": pd.DataFrame(columns=["year_month", "total_sales", "total_profit", "order_count"]),
        "category_perf": pd.DataFrame({"category": ["N/A"], "revenue": [0], "profit": [0], "orders": [0]}),
        "top_region": "N/A", "weak_region": "N/A",
        "customer_segments": pd.DataFrame({"customer_segment": ["N/A"], "cnt": [0]}),
        "top_products": pd.DataFrame({"product_name": ["N/A"], "total": [0], "units_sold": [0]}),
        "vendor_perf": pd.DataFrame({"vendor_name": ["N/A"], "vendor_gross_sales": [0], "refund_pct": [0]}),
        "top_returners": pd.DataFrame(),
        "high_risk_customers": 0, "slow_moving_products": 0,
        "query_errors": query_errors,
    }
