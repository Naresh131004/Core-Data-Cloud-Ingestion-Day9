# Enterprise Paginated API Ingestion Core Engine

## 🏗️ Architectural Layout Blueprint
A production-grade, environment-aware data extraction pipeline designed to pull multi-page JSON payloads, handle operational errors defensively, and flatten structural nesting on the fly into clean analytical layers.

## 🛠️ Tech Stack & Processing Engines
* **Language Layer:** Python 3.x
* **Network & Ingestion Adapter:** Requests (with exponential timeout backoffs)
* **Configuration Decoupling Tier:** Python-Dotenv Core Interface
* **Environment Isolation:** Local Configuration Matrix (Secured via .gitignore blocks)

## ⚡ Operational Pipeline Features
1. **Decoupled Configurations:** Pipeline architecture remains a generic engine. All endpoints, limits, and file paths are loaded at runtime out of OS memory.
2. **Dynamic Pagination:** Implements an active procedural while loop that terminates gracefully when bounds limits or empty endpoint array structures are hit.
3. **Data Quality Sanitation:** Automatically handles messy string trims, uppercase conversions, and injects consistent UNIX epoch processing execution timestamps.

## How to Run
1. Ensure Docker Desktop is installed and running.
2. Create a `.env` file in the root directory and add your variables:
   `API_TARGET_URL=...`
3. Build the image:
   `docker build -t data-ingest-engine .`
4. Run the container:
   `docker run --env-file .env data-ingest-engine`