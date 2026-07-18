# Cloud Data Pipeline: Serverless Databricks ETL to AWS S3 Data Lake

An end-to-end cloud data engineering pipeline built to ingest raw transactional REST API payloads, process them via distributed memory compute engines, and build a structured warehouse layer. 

This project implements the industry-standard **Medallion Architecture**, optimizing unstructured row data into highly efficient columnar storage files ready for downstream analytics.

---

## 🛠️ System Architecture & Workflow

The pipeline utilizes decoupled compute and storage infrastructure to maximize scalability and cost-efficiency:

1. **Ingestion (Bronze Layer):** Raw API JSON datasets are safely uploaded into Databricks Workspace storage blocks, preserving their historical structural state.
2. **Compute Engine:** Apache Spark distributed compute clusters isolate, deserialize, and enforce strict relational schemas on multi-line nested array formats.
3. **Data Transformation & Standardization:** Processing stages fix timestamp epoch codes into human-readable corporate metrics and normalize alpha strings.
4. **Storage Output (Silver Layer):** Standardized DataFrames are optimized and written back to a secure Amazon S3 Data Lake bucket partitioned in the columnar **Apache Parquet** format.

---

## 🚀 Cloud Notebook Implementation Layout

Due to security compliance hardening native to **Databricks Serverless Compute**, direct low-level modification of the Java Spark Context (`_jsc`) or global `fs.s3a` configuration overrides are blocked. This production code bypasses global platform barriers by applying **DataFrame-Scoped Option Passing** directly inside the pipeline task chain.

### Step 1: Data Ingestion & Scoped Cloud Authentication
```python
# Securely stage pipeline parameters
AWS_ACCESS_KEY = "<Your-Amazon-Access_key>"
AWS_SECRET_KEY = "<Your-Amazon-Secret-Access_key>"
S3_INPUT_PATH  = "/Workspace/Users/naresh-data-pipeline/extracted_transactions.json"

# Ingest multi-line JSON while injecting targeted cloud configurations
df_raw = (spark.read
          .option("multiLine", "true")
          .json(S3_INPUT_PATH))

print("=== BRONZE ZONE: RAW STORAGE SCHEMA ===")
df_raw.printSchema()