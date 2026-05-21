# 📈 Nifty 50 Stock ETL Pipeline

An automated end-to-end **ETL pipeline** built with **Apache Airflow** on **Docker** that fetches 60 days of historical stock data for top 10 Nifty 50 companies, transforms it using Pandas, stores results in PostgreSQL, and generates multi-sheet Excel reports.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| **Apache Airflow (Astro)** | Pipeline orchestration and task scheduling |
| **Docker** | Containerized Airflow environment |
| **Python** | Core programming language |
| **yfinance** | Fetch historical stock data from Yahoo Finance |
| **Pandas** | Data transformation and analysis |
| **PostgreSQL** | Persistent data storage |
| **openpyxl** | Multi-sheet Excel report generation |
| **psycopg2** | Python-PostgreSQL connector |
| **SQL** | Database schema, queries, and data retrieval |

---

## Stocks Tracked

Top 10 Nifty 50 companies across 6 sectors — Energy, IT, Banking, FMCG, Telecom, and Infrastructure.

| Stock | Company | Sector |
|-------|---------|--------|
| RELIANCE.NS | Reliance Industries | Energy |
| TCS.NS | Tata Consultancy Services | IT |
| HDFCBANK.NS | HDFC Bank | Banking |
| INFY.NS | Infosys | IT |
| ICICIBANK.NS | ICICI Bank | Banking |
| HINDUNILVR.NS | Hindustan Unilever | FMCG |
| SBIN.NS | State Bank of India | Banking |
| BHARTIARTL.NS | Bharti Airtel | Telecom |
| KOTAKBANK.NS | Kotak Mahindra Bank | Banking |
| LT.NS | Larsen & Toubro | Infrastructure |

---

## Pipeline Architecture

```
yfinance API (Yahoo Finance)
          ↓
    ┌─────────────┐
    │ extract_task│  → Fetches 60 days OHLCV data for 10 stocks
    └──────┬──────┘
           ↓
    ┌─────────────────┐
    │ transform_task  │  → Cleans data, calculates Price Change %,
    │                 │    MA_7 and MA_30 moving averages
    └────────┬────────┘
             ↓
    ┌──────────────┐
    │  load_task   │  → Stores in PostgreSQL + exports Excel report
    └──────────────┘
```

---

## Project Structure

```
Nifty-50-Stock-ETL-Pipeline/
├── dags/
│   └── stock_etl.py          # Main DAG — Extract, Transform, Load tasks
├── include/
│   └── stock_report.xlsx     # Generated Excel report (after pipeline run)
├── tests/dags/               # DAG unit tests
├── Dockerfile                # Astro Runtime Docker image
├── requirements.txt          # Python dependencies
└── packages.txt              # OS-level dependencies
```

---

## Transformations Performed

- **Price Change %** — Daily percentage change in closing price
- **MA_7** — 7-day moving average to track short-term trend
- **MA_30** — 30-day moving average to track long-term trend
- **Sector Performance** — Average close, volume, and change % grouped by sector

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

### Run the Pipeline

```bash
# Clone the repository
git clone https://github.com/ShirishS02/Nifty-50-Stock-ETL-Pipeline.git
cd Nifty-50-Stock-ETL-Pipeline

# Start Airflow
astro dev start
```

Open `http://localhost:<port>` in your browser, find the **`nifty50_stock_etl`** DAG, and click **▶️ Trigger DAG**.

> Set Airflow credentials via environment variables — never hardcode them.

---

## Key Engineering Decisions

- **File-based intermediate storage** — Used `/tmp/` CSV files instead of Airflow XCom (limited to ~48KB) to efficiently pass large datasets between tasks.
- **Composite Primary Key** — `PRIMARY KEY (date, stock)` in PostgreSQL prevents duplicate entries on pipeline reruns.
- **NaN Handling** — Converted Pandas NaN to Python `None` before DB insertion for PostgreSQL compatibility.

---

## Phase 2 Roadmap

- [ ] Deploy on **AWS EC2** with automated daily scheduling
- [ ] Migrate PostgreSQL to **AWS RDS**
- [ ] Integrate real-time API (**Upstox** or **Zerodha Kite**)
- [ ] Add **Power BI dashboard** for visualization
- [ ] Add **email alerts** on pipeline failure

---

> Built with ❤️ by [ShirishS02](https://github.com/ShirishS02)
