

import asyncio

from coinflow.streams.ticker import ticker
from coinflow.core.logger import setup_logger
from coinflow.db.sqlite import get_all_tickers, init_db


logger = setup_logger()

async def main():
    # 1. Initialize the database (ensures the table exists)
    init_db()
    
    # 2. Runs the ticker stream (BTCUSDT for 10s)
    await ticker()
    
    # 3. After the stream stops, shows what was saved in the database 
    rows = get_all_tickers()
    logger.info("📊 Saved records in the database:")
    
    for row in rows:
        logger.info(row)
        
if __name__ == "__main__":
    asyncio.run(main())