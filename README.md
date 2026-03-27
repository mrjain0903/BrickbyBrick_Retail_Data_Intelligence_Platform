# GlobalMart E-Commerce Analytics

Hackathon project: **medallion data pipeline** on **Databricks** (Delta Live Tables) plus a **Gen AI layer** for data quality, returns risk, product Q&A, and executive insights.

---

## Overview

| Layer | Catalog | What it does |
|--------|---------|----------------|
| **Bronze** | `globalmart.bronze` | Auto Loader ingests CSV/JSON from a Unity Catalog Volume; adds lineage columns. |
| **Silver** | `globalmart.silver` | Harmonizes IDs, dates, money, segments; **quarantine** bad rows with `failure_reasons`; **`quality_run_summary`**. |
| **Gold** | `globalmart.gold` | Star schema (**dims** + **facts**) and **KPI aggregates** for BI and AI. |
| **Gen AI** | Writes to `globalmart.gold.*` | LLM + (optional) RAG on top of Silver/Gold outputs. |

---

## Repository layout

```
├── GlobalMart_Retail_Data/     # Sample source files (CSV + JSON, multi-region)
├── Jobs/
│   └── Globalmart Ecommerce Pipeline.yaml   # Databricks bundle / pipeline job definition
└── Notebooks/
    ├── Bronze.py               # DLT: Bronze ingestion
    ├── Silver.py               # DLT: Silver + quarantine + quality summary
    ├── Gold.py                 # DLT: dimensions, facts, aggregates
    ├── UC1 Designing the AI Data Quality Reporter.ipynb
    ├── UC2 Engineering the Returns Fraud Investigator.ipynb
    ├── UC3 Building the Product Intelligence Assistant.ipynb
    └── UC4 Synthesizing Executive Business Intelligence.ipynb
```

**Detailed architecture** (data flow, components, DQ rules, Gold tables, Gen AI I/O): see **[`GlobalMart_Architecture.md`](./GlobalMart_Architecture.md)**.

---

## Gen AI use cases

| UC | Business need | Main inputs | Output table (Gold) |
|----|-----------------|-------------|------------------------|
| **UC1** | Finance / audit narratives for Silver rejections | `globalmart.silver.quarantine_*` | `globalmart.gold.dq_audit_report` |
| **UC2** | Flag suspicious return patterns + investigation brief | `fact_return`, `dim_customer`, `fact_sales` | `globalmart.gold.flagged_return_customers` |
| **UC3** | Natural-language product & vendor Q&A (RAG) | `dim_product`, `dim_vendor`, product/region & vendor KPI aggs | `globalmart.gold.rag_query_history` |
| **UC4** | Executive summaries from KPIs | `agg_monthly_revenue_by_region`, `agg_vendor_return_rate`, `agg_product_region_monthly` | `globalmart.gold.ai_business_insights` |

**Model:** `databricks-gpt-oss-20b` (Databricks serving endpoint). UC3 also uses **Sentence Transformers** + **FAISS** for retrieval.

---

## Prerequisites

- **Databricks** workspace with **Unity Catalog**
- Catalog **`globalmart`** and schemas **`bronze`**, **`silver`**, **`gold`**
- Raw data under a Volume path consistent with **`Bronze.py`** (e.g. `GlobalMart_Retail_Data` under your configured landing path)
- For Gen AI notebooks: cluster with **OpenAI-compatible** access to the workspace serving endpoint; UC3 may need **GPU/CPU** for embeddings (per cluster policy)

---

## How to run

1. **Upload** `GlobalMart_Retail_Data` to the Volume path expected by `Bronze.py`.
2. **Import** `Notebooks/Bronze.py`, `Silver.py`, and `Gold.py` as **DLT pipeline** modules (separate pipelines or combined per your design — *do not mix Bronze into Silver unless intended*).
3. **Run** the pipeline(s) to materialize Bronze → Silver → Gold.
4. **Run** UC1–UC4 notebooks in order after Gold exists (and after Silver quarantine exists for UC1).

**Job / bundle:** `Jobs/Globalmart Ecommerce Pipeline.yaml` is a **Databricks asset bundle** reference — adjust `root_path`, `libraries`, and workspace paths to match your environment before deploy.

---

## Gold analytics (summary)

- **Dimensions:** `dim_region`, `dim_date`, `dim_customer`, `dim_product`, `dim_vendor`
- **Facts:** `fact_sales` (order line), `fact_return` (return event)
- **Aggregates:** `agg_monthly_revenue_by_region`, `agg_customer_return_summary`, `agg_vendor_return_rate`, `agg_product_region_monthly`

---
