import sqlite3
from pathlib import Path

DB_PATH = Path("./coinflow/db/coinflow.db")

def get_connection():
    """
    Creates and retruns a connection to the SQLite database.
    """
    return sqlite3.connect(DB_PATH)

def init_db():
    """
    Creates the 'tickers' table if it doesn't exist yet.
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tickers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT,
                event_timestamp INTEGER,
                symbol TEXT,
                price_change REAL,
                price_change_percent REAL,
                weighted_avg_price REAL,
                previous_close_price REAL,
                current_close_price REAL,
                close_trade_qty REAL,
                best_bid_price REAL,
                best_bid_qty REAL,
                best_ask_price REAL,
                best_ask_qty REAL,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                total_traded_base_asset REAL,
                total_traded_quote_asset REAL,
                open_time INTEGER,
                close_time INTEGER,
                first_trade_id INTEGER,
                last_trade_id INTEGER,
                total_number_of_trades INTEGER
            )
        """)
        conn.commit()
        
def save_ticker(data: dict):
    """
    Receives a dictionary (your 'result') and inserts it into the database.
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO tickers (
                event_type, event_timestamp, symbol,
                price_change, price_change_percent,
                weighted_avg_price, previous_close_price, current_close_price,
                close_trade_qty, best_bid_price, best_bid_qty,
                best_ask_price, best_ask_qty, open_price, high_price, low_price,
                total_traded_base_asset, total_traded_quote_asset,
                open_time, close_time, first_trade_id, last_trade_id, total_number_of_trades
            )
            VALUES (
                :event_type, :event_timestamp, :symbol,
                :price_change, :price_change_percent,
                :weighted_avg_price, :previous_close_price, :current_close_price,
                :close_trade_qty, :best_bid_price, :best_bid_qty,
                :best_ask_price, :best_ask_qty, :open_price, :high_price, :low_price,
                :total_traded_base_asset, :total_traded_quote_asset,
                :open_time, :close_time, :first_trade_id, :last_trade_id, :total_number_of_trades
            )
        """, data)
        conn.commit()
        
def get_all_tickers():
    """
    Returns all saved records (quick test).
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tickers")
        return cursor.fetchall()