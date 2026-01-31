# Multi-Agent Excel Analytics

This project is a web application for uploading, profiling, cleaning, and analyzing tabular datasets (Excel and CSV files) using a FastAPI backend and a Streamlit frontend.
Each dataset is processed per user, with full history of cleaning runs stored on the backend. The system supports, multi-sheet Excel ingestion , dataset profiling , automated and LLM-assisted data cleaning , persistent cleaning runs , exporting cleaned data as combined Excel files


## Requirements
	•	Docker, Docker Compose
	•	Python 3.9+ (fastapi, uvicorn, sqlalchemy, psycopg2, pandas, pyarrow, xlsxwriter, pydantic)

All Python dependencies are listed in requirements.txt.


## Setup
### 1. Clone the repository
```bash
git clone https://github.com/konansul/Multi-Agent-Excel-Analytics
cd Multi-Agent-Excel-Analytics
```

### 2. Start PostgreSQL through Docker.
PostgreSQL is started via Docker Compose, this will start at port 5433.

```bash
docker compose up -d
```

### 3. Create and configure environment variables
This project uses environment variables for database connection, authentication, storage, and LLM access. Create a .env file in the project root directory (the same level as README.md):

```bash
touch .env
```

Add the following variables to the .env file, GEMINI_API_KEY is required only if LLM-assisted cleaning is enabled.

```bash
GEMINI_API_KEY=your_gemini_api_key_here

DATABASE_URL=postgresql+psycopg2://excel:excel@localhost:5433/excel_analytics

JWT_SECRET_KEY=long-random-string-at-least-32chars
JWT_EXPIRE_MINUTES=60
JWT_ALGORITHM=HS256
REFRESH_TOKEN_EXPIRE_DAYS=7
```

### 4. Create virtual environment and install requirements.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
### 5. Start the backend API server
The backend will be available at: http://127.0.0.1:8000, swagger documentation: http://127.0.0.1:8000/docs

```bash
 uvicorn backend.api.main:app --reload --reload-dir backend --port 8000
```

### 6. Start Streamlit frontend 
In a separate terminal, start the Streamlit frontend, which will be available at http://localhost:8501

```bash
streamlit run frontend/main_streamlit.py
```
## Database Schema
The backend uses a PostgreSQL database to persist users, datasets, profiling results, and cleaning executions in a fully reproducible way. Each registered user has isolated ownership over their uploaded datasets, generated profiles, and cleaning runs. Uploaded Excel and CSV files are represented as datasets, where each Excel sheet is stored and processed independently. Profiling results are stored separately and capture structural and statistical signals used to guide cleaning decisions. Every execution of the cleaning pipeline is recorded as a cleaning run, including its status, generated artifacts, and reports. Multiple cleaning runs can exist for the same dataset, allowing history tracking and safe re-execution with different policies. The schema is designed to preserve the full lineage from raw data to cleaned outputs, even across user logins. Database records reference large artifacts stored on disk, combining transactional metadata with efficient file storage. The overall schema is depicted in the figure below:

<img width="2940" height="1912" alt="image" src="https://github.com/user-attachments/assets/d6cdcee7-6583-4ba1-a092-e54c64b3918f" />

## Project structure

```bash
Multi-Agent-Excel-Analytics/
│
├── backend/                             # FastAPI backend
│   ├── api/                             # API routers and HTTP layer
│   │   ├── auth.py                      # Authentication endpoints
│   │   ├── cleaning.py                  # Cleaning runs API
│   │   ├── datasets.py                  # Dataset upload and access
│   │   ├── policy.py                    # Cleaning policy endpoints
│   │   ├── profiling.py                 # Profiling endpoints
│   │   ├── storage.py                   # API-level storage helpers
│   │   └── main.py                      # FastAPI application entrypoint
│   │
│   ├── app/                             # Core business logic
│   │   ├── ingestion/                   # Dataset ingestion (Excel/CSV parsing)
│   │   │   └── dataset_loader.py
│   │   │
│   │   ├── cleaning_steps/                    # 10-stage data cleaning pipeline
│   │   │   ├── main_pipeline.py               # Orchestrates the full pipeline
│   │   │   ├── _01_normalize.py
│   │   │   ├── _02_trim_strings.py
│   │   │   ├── _03_standardize_missing.py
│   │   │   ├── _04_cast_types.py
│   │   │   ├── _05_encode_booleans.py
│   │   │   ├── _06_drop_rules.py
│   │   │   ├── _07_datetime_inference.py
│   │   │   ├── _08_deduplicate.py
│   │   │   ├── _09_outliers.py
│   │   │   └── _10_impute_missing.py
│   │   │
│   │   ├── cleaning_agent/              # Cleaning policy engine (rule-based + LLM)
│   │   │   ├── schemas.py                # CleaningPlan schema and validation
│   │   │   ├── cleaning_policy_agent.py  # Public policy builder API
│   │   │   ├── cleaning_policy_rule_based.py
│   │   │   ├── cleaning_policy_llm.py
│   │   │   ├── cleaning_policy_utils.py  # Safety, clamping, coercion
│   │   │   └── llm_client.py             # Gemini LLM client wrapper
│   │   │
│   │   └── profiling/                   # Dataset profiling logic
│   │       └── profiling.py
│   │
│   ├── database/                        # Database and storage layer
│   │   ├── db.py                        # SQLAlchemy session
│   │   ├── models.py                    # ORM models
│   │   ├── security.py                  # JWT / auth helpers
│   │   └── storage.py                   # Local blob storage + JSON serialization
│   │
│   ├── test_data/                       # Sample datasets
│   └── test_scripts/                    # Backend tests and experiments
│
├── frontend/                            # Streamlit frontend
│   ├── main_streamlit.py                # Streamlit application entrypoint
│   └── ui/
│       ├── _00_tab_authentication.py    # Login and registration
│       ├── _01_tab_excel_upload.py      # File upload and ingestion
│       ├── _02_tab_cleaning.py           # Cleaning execution UI
│       ├── _03_tab_signals.py            # Profiling and signal visualization
│       ├── _04_tab_visualization.py      # Charts and plots
│       ├── _05_save_all_files.py         # Download cleaned datasets and reports
│       ├── components.py                # Shared UI components
│       └── data_access.py               # Frontend → Backend API client
│
├── storage/                             # Persistent local storage (user-scoped)
│   └── users/
│       └── usr_<user_id>/
│           ├── datasets/                # Uploaded raw datasets
│           │   └── ds_<dataset_id>/
│           │       ├── raw.bin
│           │       ├── raw.parquet
│           │       └── current.parquet
│           │
│           └── runs/                    # Cleaning runs history
│               └── run_<run_id>/
│                   ├── cleaned.parquet
│                   ├── cleaned.xlsx
│                   └── report.json
│
├── docker-compose.yml                   # PostgreSQL
├── requirements.txt                     # Python dependencies
├── README.md
└── LICENSE
```

## API Endpoints

### 1. Authentication

```bash
POST   /v1/auth/register     Register new user
POST   /v1/auth/login        Login
GET    /v1/auth/me           Get current user
POST   /v1/auth/logout       Logout (client-side token removal)
```

### 2. Datasets

```bash
GET    /v1/datasets                         List all datasets for the current user
POST   /v1/datasets                         Upload a new dataset (Excel or CSV)=
GET    /v1/datasets/{dataset_id}            Get dataset metadata
GET    /v1/datasets/{dataset_id}/preview    Preview dataset rows
GET    /v1/datasets/{dataset_id}/download   Download dataset (raw or cleaned)
```

### 3. Profiling

```bash
POST   /v1/profiling                  Run profiling
GET    /v1/profiling/{profile_id}     Get profiling report
```

### 4. Policy

```bash
POST   /v1/policy/suggest             Suggest cleaning policy
```

### 5. Cleaning

```bash
GET    /v1/cleaning/runs                          List user cleaning runs
POST   /v1/cleaning/runs                          Run cleaning
GET    /v1/cleaning/runs/{run_id}                 Get run status
GET    /v1/cleaning/runs/{run_id}/report          Get run report
GET    /v1/cleaning/runs/{run_id}/artifacts/{name} Download artifact
DELETE /v1/cleaning/runs/{run_id}                 Delete run
```

Notes: Excel files with multiple sheets are ingested as multiple datasets. Cleaning can be rule-based or LLM-assisted. Cleaning history persists across sessions per user. CSV files are treated as single-sheet datasets. Frontend and backend are intentionally decoupled and run independently.

