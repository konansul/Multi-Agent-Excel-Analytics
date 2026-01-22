# Multi-Agent Excel Analytics

This project is a web application for uploading, profiling, cleaning, and analyzing tabular datasets (Excel and CSV files) using a FastAPI backend and a Streamlit frontend.
Each dataset is processed per user, with full history of cleaning runs stored on the backend. The system supports, multi-sheet Excel ingestion , dataset profiling , automated and LLM-assisted data cleaning , persistent cleaning runs , exporting cleaned data as combined Excel files


## Requirements
	•	Docker, Docker Compose
	•	Python 3.9+ (fastapi, uvicorn, sqlalchemy, psycopg2, pandas, pyarrow, xlsxwriter, pydantic)

All Python dependencies are listed in requirements.txt.


## Setup
### 1. Clone the repository

git clone https://github.com/konansul/Multi-Agent-Excel-Analytics
cd Multi-Agent-Excel-Analytics

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


## Project structure

```bash
Multi-Agent-Excel-Analytics/
│
├── backend/                        # FastAPI backend
│   ├── api/                        # API routers (auth, datasets, profiling, policy, cleaning)
│   │   ├── auth.py
│   │   ├── cleaning.py
│   │   ├── datasets.py
│   │   ├── policy.py
│   │   ├── profiling.py
│   │   ├── storage.py
│   │   └── main.py                 # FastAPI application entrypoint
│   │
│   ├── app/                        # Core business logic
│   │   ├── ingestion/              # Dataset ingestion (Excel/CSV parsing)
│   │   ├── cleaning/               # Cleaning pipeline steps
│   │   │   ├── main_pipeline.py
│   │   │   ├── _01_snapshots.py
│   │   │   ├── _02_normalize.py
│   │   │   ├── _03_drop_rules.py
│   │   │   ├── _04_datetime_inference.py
│   │   │   ├── _05_impute_missing.py
│   │   │   └── _06_differences.py
│   │   ├── profiling/              # Dataset profiling logic
│   │   └── agents/                 # Policy agents (rule-based and LLM-based)
│   │
│   ├── database/                   # Database layer
│   │   ├── db.py                   # SQLAlchemy session
│   │   ├── models.py               # ORM models
│   │   ├── security.py             # Auth / JWT utilities
│   │   └── storage.py              # Storage abstractions
│   │
│   ├── test_data/                  # Sample datasets
│   └── test_scripts/               # Backend tests and experiments
│
├── frontend/                       # Streamlit frontend
│   ├── main_streamlit.py           # Streamlit application entrypoint
│   └── ui/
│       ├── _00_tab_authentication.py
│       ├── _01_tab_excel_upload.py
│       ├── _02_tab_cleaning.py
│       ├── _03_tab_signals.py
│       ├── _04_tab_visualization.py
│       ├── _05_save_all_files.py
│       ├── components.py
│       └── data_access.py          # Frontend → Backend API client
│
├── storage/                        # Persistent local storage (outside backend)
│   └── users/
│       └── usr_<user_id>/
│           ├── datasets/            # Per-dataset storage
│           │   └── ds_<dataset_id>/
│           │       ├── raw.bin
│           │       ├── raw.parquet
│           │       └── current.parquet
│           │
│           └── runs/                # Cleaning runs history
│               └── run_<run_id>/
│                   ├── cleaned.parquet
│                   ├── cleaned.xlsx
│                   └── report.json
│
├── docker-compose.yml              # PostgreSQL only
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment variables template
├── README.md
└── LICENSE
```

## API Endpoints

### Authentication

```bash
POST   /v1/auth/register     Register new user
POST   /v1/auth/login        Login
GET    /v1/auth/me           Get current user
POST   /v1/auth/logout       Logout (client-side token removal)
```

### Datasets

```bash
POST   /v1/auth/register     Register new user
POST   /v1/auth/login        Login
GET    /v1/auth/me           Get current user
POST   /v1/auth/logout       Logout (client-side token removal)
```

### Profiling

```bash
POST   /v1/profiling                  Run profiling
GET    /v1/profiling/{profile_id}     Get profiling report
```

### Policy

```bash
POST   /v1/policy/suggest             Suggest cleaning policy
```

### Cleaning

```bash
GET    /v1/cleaning/runs                          List user cleaning runs
POST   /v1/cleaning/runs                          Run cleaning
GET    /v1/cleaning/runs/{run_id}                 Get run status
GET    /v1/cleaning/runs/{run_id}/report          Get run report
GET    /v1/cleaning/runs/{run_id}/artifacts/{name} Download artifact
DELETE /v1/cleaning/runs/{run_id}                 Delete run
```

Notes: Excel files with multiple sheets are ingested as multiple datasets. Cleaning can be rule-based or LLM-assisted. Cleaning history persists across sessions per user. CSV files are treated as single-sheet datasets. Frontend and backend are intentionally decoupled and run independently.

