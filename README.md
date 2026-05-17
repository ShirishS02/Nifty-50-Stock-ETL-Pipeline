# 📈 Nifty 50 Stock ETL Pipeline

An automated end-to-end **ETL (Extract, Transform, Load) pipeline** built with **Apache Airflow** on **Docker** that fetches 60 days of historical stock data for top 10 Nifty 50 companies, performs data transformation using Pandas, stores results in PostgreSQL, and generates multi-sheet Excel reports.

---

## 🎯 Project Overview

This project demonstrates a production-style data engineering pipeline that:
- **Extracts** real-time historical stock data from Yahoo Finance API (yfinance)
- **Transforms** raw data using Pandas — calculates price change %, moving averages, and sector performance
- **Loads** processed data into PostgreSQL database and exports multi-sheet Excel reports

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| **Apache Airflow (Astro)** | Pipeline orchestration and task scheduling |
| **Docker** | Containerized Airflow environment |
| **Python** | Core programming language |
| **yfinance** | Fetch historical stock data from Yahoo Finance |
| **Pandas** | Data transformation and analysis |
| **PostgreSQL** | Persistent data storage |
| **openpyxl** | Multi-sheet Excel report generation |
| **psycopg2** | Python-PostgreSQL connector |
| **Git & GitHub** | Version control |

---

## 📊 Stocks Tracked

Top 10 Nifty 50 companies across 6 sectors:

| Stock | Company | Sector |
|---|---|---|
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

## 🏗️ Pipeline Architecture

```
yfinance API (Yahoo Finance)
          ↓
    ┌─────────────┐
    │ extract_task│  → Fetches 60 days OHLCV data for 10 stocks
    │             │  → Saves to /tmp/nifty50_extracted.csv
    └──────┬──────┘
           ↓
    ┌─────────────────┐
    │ transform_task  │  → Cleans data, rounds values
    │                 │  → Calculates Price Change %
    │                 │  → Calculates MA_7 and MA_30
    │                 │  → Saves to /tmp/nifty50_transformed.csv
    └────────┬────────┘
             ↓
    ┌──────────────┐
    │  load_task   │  → Stores in PostgreSQL
    │              │  → Exports 4-sheet Excel report
    │              │  → Cleans up temp files
    └──────────────┘
```

---

## 📁 Project Structure

```
Stock-ETL-Pipeline/
│
├── dags/
│   └── stock_etl.py          # Main DAG with Extract, Transform, Load tasks
│
├── include/
│   └── stock_report.xlsx     # Generated Excel report (after pipeline run)
│
├── Dockerfile                # Astro Runtime Docker image
├── requirements.txt          # Python dependencies
├── packages.txt              # OS-level dependencies
└── README.md                 # Project documentation
```

---

## 📈 Transformations Performed

### 1. Price Change %
```
Formula: ((Today's Close - Yesterday's Close) / Yesterday's Close) × 100
Example: ((2727 - 2700) / 2700) × 100 = +1.0%
```

### 2. 7-Day Moving Average (MA_7)
```
Formula: Average of last 7 trading days closing prices
Purpose: Shows short-term price trend direction
Signal:  Close > MA_7 = Bullish ↑ | Close < MA_7 = Bearish ↓
```

### 3. 30-Day Moving Average (MA_30)
```
Formula: Average of last 30 trading days closing prices
Purpose: Shows long-term price trend direction
Signal:  MA_7 crosses above MA_30 = Golden Cross (BUY signal)
```

---

## 📊 Excel Report Structure

| Sheet | Content |
|---|---|
| **Raw Data** | Complete OHLCV data — Date, Stock, Symbol, Sector, Open, High, Low, Close, Volume |
| **Gainers & Losers** | 60-day performance — Start Price, End Price, % Change sorted best to worst |
| **Moving Averages** | Daily Close, MA_7, MA_30 for trend analysis |
| **Sector Performance** | Average Close, Volume, and Change % grouped by sector |

---

## 🗄️ PostgreSQL Table Schema

```sql
CREATE TABLE nifty50_stock_data (
    date             TEXT,
    stock            TEXT,
    symbol           TEXT,
    sector           TEXT,
    open             FLOAT,
    high             FLOAT,
    low              FLOAT,
    close            FLOAT,
    volume           BIGINT,
    price_change_pct FLOAT,
    ma_7             FLOAT,
    ma_30            FLOAT,
    PRIMARY KEY (date, stock)
);
```

### Useful SQL Queries

```sql
-- Top gaining stocks
SELECT stock,
       ROUND(((MAX(close) - MIN(close)) / MIN(close) * 100)::numeric, 2) AS gain_pct
FROM nifty50_stock_data
GROUP BY stock
ORDER BY gain_pct DESC;

-- Sector performance comparison
SELECT sector,
       ROUND(AVG(price_change_pct)::numeric, 2) AS avg_daily_change
FROM nifty50_stock_data
GROUP BY sector
ORDER BY avg_daily_change DESC;

-- Most actively traded stocks
SELECT stock,
       ROUND(AVG(volume)) AS avg_daily_volume
FROM nifty50_stock_data
GROUP BY stock
ORDER BY avg_daily_volume DESC;

-- Bullish stocks (price above 30-day average)
SELECT DISTINCT stock, close, ma_30,
       ROUND((close - ma_30)::numeric, 2) AS above_ma_by
FROM nifty50_stock_data
WHERE date = (SELECT MAX(date) FROM nifty50_stock_data)
AND close > ma_30
ORDER BY above_ma_by DESC;
```

---

## 🚀 Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running
- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/ShirishS02/Stock-ETL-Pipeline.git

# 2. Navigate to project folder
cd Stock-ETL-Pipeline

# 3. Start Airflow
astro dev start
```

### Check Running Port
```bash
astro dev ps
```

### Access Airflow UI
Open browser and go to:
```
http://localhost:<port>
```
> Default credentials: **Username:** `admin` | **Password:** `admin`

### Run the Pipeline
1. Find **`nifty50_stock_etl`** DAG in Airflow UI
2. Click **▶️ Play button** → **Trigger DAG**
3. Watch all 3 tasks turn green ✅
4. Check Excel report in `include/stock_report.xlsx`

---

## ⚙️ Docker Services

| Container | Role |
|---|---|
| **Postgres** | Airflow metadata + stock data storage |
| **Scheduler** | Monitors and triggers DAG tasks |
| **DAG Processor** | Parses DAG Python files |
| **API Server** | Serves Airflow UI |
| **Triggerer** | Handles deferred tasks |

---

## 🔑 Key Engineering Decisions

### File-Based Intermediate Storage
Instead of Airflow's XCom (which has a ~48KB limit), we used temporary CSV files at `/tmp/` to pass data between tasks. This handles large datasets efficiently and follows production best practices.

### Composite Primary Key
Used `PRIMARY KEY (date, stock)` in PostgreSQL to prevent duplicate entries when the pipeline runs multiple times — ensuring data integrity.

### NaN Handling
Added `df.where(pd.notnull(df), None)` before database insertion to convert Pandas NaN values to Python None, which PostgreSQL accepts correctly.

---

## 📅 Phase 2 Roadmap

- [ ] Deploy on **AWS EC2** with automated daily scheduling
- [ ] Migrate PostgreSQL to **AWS RDS**
- [ ] Integrate real-time API (**Upstox** or **Zerodha Kite**)
- [ ] Add **Power BI dashboard** for data visualization
- [ ] Add **email alerts** on pipeline failure

---

## 👨‍💻 Author

**Shirish S.**
- GitHub: [@ShirishS02](https://github.com/ShirishS02)

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).
