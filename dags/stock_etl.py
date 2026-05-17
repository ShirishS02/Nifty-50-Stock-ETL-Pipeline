from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import yfinance as yf
import pandas as pd
import psycopg2
import numpy as np
import os

# Top 10 Nifty 50 Stocks
STOCKS = {
    "Reliance": "RELIANCE.NS",
    "TCS": "TCS.NS",
    "HDFC Bank": "HDFCBANK.NS",
    "Infosys": "INFY.NS",
    "ICICI Bank": "ICICIBANK.NS",
    "HUL": "HINDUNILVR.NS",
    "SBI": "SBIN.NS",
    "Airtel": "BHARTIARTL.NS",
    "Kotak Bank": "KOTAKBANK.NS",
    "L&T": "LT.NS"
}

SECTORS = {
    "Reliance": "Energy",
    "TCS": "IT",
    "HDFC Bank": "Banking",
    "Infosys": "IT",
    "ICICI Bank": "Banking",
    "HUL": "FMCG",
    "SBI": "Banking",
    "Airtel": "Telecom",
    "Kotak Bank": "Banking",
    "L&T": "Infrastructure"
}

EXTRACT_PATH = "/tmp/nifty50_extracted.csv"
TRANSFORM_PATH = "/tmp/nifty50_transformed.csv"


def extract():
    all_data = []

    for name, symbol in STOCKS.items():
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="60d")
        df = df.reset_index()
        df['stock'] = name
        df['symbol'] = symbol
        df['sector'] = SECTORS[name]
        all_data.append(df)

    combined = pd.concat(all_data, ignore_index=True)

    combined['Date'] = pd.to_datetime(combined['Date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')

    combined.to_csv(EXTRACT_PATH, index=False)
    print(f"Extracted {len(combined)} rows. Saved to {EXTRACT_PATH}")


def transform():
    df = pd.read_csv(EXTRACT_PATH)

    # Clean data
    df = df.dropna(subset=['Close'])
    df['Close'] = df['Close'].round(2)
    df['Open'] = df['Open'].round(2)
    df['High'] = df['High'].round(2)
    df['Low'] = df['Low'].round(2)
    df['Volume'] = df['Volume'].astype(int)

    # Price change %
    df = df.sort_values(['stock', 'Date'])
    df['price_change_pct'] = df.groupby('stock')['Close'].pct_change() * 100
    df['price_change_pct'] = df['price_change_pct'].round(2)

    # 7-day moving average
    df['MA_7'] = df.groupby('stock')['Close'].transform(
        lambda x: x.rolling(window=7).mean()
    ).round(2)

    # 30-day moving average
    df['MA_30'] = df.groupby('stock')['Close'].transform(
        lambda x: x.rolling(window=30).mean()
    ).round(2)

    df.to_csv(TRANSFORM_PATH, index=False)
    print(f"Transformed {len(df)} rows. Saved to {TRANSFORM_PATH}")


def load():
    df = pd.read_csv(TRANSFORM_PATH)

    # ✅ Replace NaN with None for PostgreSQL compatibility
    df = df.where(pd.notnull(df), None)

    # Store in PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="postgres",
        user="postgres",
        password="postgres",
        port="5432"
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nifty50_stock_data (
            date TEXT,
            stock TEXT,
            symbol TEXT,
            sector TEXT,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            price_change_pct FLOAT,
            ma_7 FLOAT,
            ma_30 FLOAT,
            PRIMARY KEY (date, stock)
        )
    """)

    inserted = 0
    skipped = 0
    for _, record in df.iterrows():
        cursor.execute("""
            INSERT INTO nifty50_stock_data 
            (date, stock, symbol, sector, open, high, low, close, 
             volume, price_change_pct, ma_7, ma_30)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, stock) DO NOTHING;
        """, (
            record['Date'], record['stock'], record['symbol'],
            record['sector'], record['Open'], record['High'],
            record['Low'], record['Close'], int(record['Volume']),
            record.get('price_change_pct'), record.get('MA_7'),
            record.get('MA_30')
        ))
        if cursor.rowcount > 0:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    cursor.close()
    conn.close()
    print(f"PostgreSQL: {inserted} rows inserted, {skipped} rows skipped (duplicates)")

    # Excel Export - 4 Sheets
    excel_path = "/usr/local/airflow/include/stock_report.xlsx"
    os.makedirs(os.path.dirname(excel_path), exist_ok=True)

    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:

        # Sheet 1 - Raw Data
        raw = df[['Date', 'stock', 'symbol', 'sector',
                  'Open', 'High', 'Low', 'Close', 'Volume']]
        raw.to_excel(writer, sheet_name='Raw Data', index=False)

        # Sheet 2 - Top Gainers & Losers
        summary = df.groupby('stock').agg(
            Sector=('sector', 'first'),
            Start_Price=('Close', 'first'),
            End_Price=('Close', 'last'),
        ).reset_index()
        summary['60d_change_pct'] = (
            (summary['End_Price'] - summary['Start_Price'])
            / summary['Start_Price'] * 100
        ).round(2)
        summary = summary.sort_values('60d_change_pct', ascending=False)
        summary.to_excel(writer, sheet_name='Gainers & Losers', index=False)

        # Sheet 3 - Moving Averages
        ma_df = df[['Date', 'stock', 'Close', 'MA_7', 'MA_30']].dropna()
        ma_df.to_excel(writer, sheet_name='Moving Averages', index=False)

        # Sheet 4 - Sector Performance
        sector = df.groupby('sector').agg(
            Avg_Close=('Close', 'mean'),
            Avg_Volume=('Volume', 'mean'),
            Avg_Change_Pct=('price_change_pct', 'mean')
        ).reset_index()
        sector = sector.round(2)
        sector = sector.sort_values('Avg_Change_Pct', ascending=False)
        sector.to_excel(writer, sheet_name='Sector Performance', index=False)

    print(f"Excel report saved to: {excel_path}")

    # Cleanup temp files
    os.remove(EXTRACT_PATH)
    os.remove(TRANSFORM_PATH)


with DAG(
    dag_id='nifty50_stock_etl',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load
    )

    extract_task >> transform_task >> load_task