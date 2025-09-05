import asyncio
import os
import logging

from binance_sdk_spot.spot import (
    Spot,
    SPOT_WS_STREAMS_PROD_URL,
    ConfigurationWebSocketStreams,
)


# Configure logging
logging.basicConfig(level=logging.INFO)

# Create configuration for the WebSocket Streams
configuration_ws_streams = ConfigurationWebSocketStreams(
    stream_url=os.getenv("STREAM_URL", SPOT_WS_STREAMS_PROD_URL)
)

# Initialize Spot client
client = Spot(config_ws_streams=configuration_ws_streams)

def handle_message(data):
    # transforma em dicionário
    d = data.to_dict()
    
    result = {
        'event_type': d['e'],                   # STRING: event's type , ex: '24hrTicker'
        'event_timestamp': d['E'],              # INT: event timestamp  em ms
        'symbol': d['s'],                       # STRING: trading pair, e.g., 'BTCUSDT'
        'price_change': d['p'],                 # STRING: price change in the last 24h
        'price_change_percent': d['P'],         # STRING: percentage price change in the last 24h
        'weighted_avg_price': d['w'],           # STRING: weighted average price
        'previous_close_price': d['x'],         # STRING: previous closing price
        'current_close_price': d['c'],          # STRING: current closing price
        'close_trade_qty': d['Q'],              # STRING: quantity of the last closing trade
        'best_bid_price': d['b'],               # STRING: best bid price
        'best_bid_qty': d['B'],                 # STRING: available quantity at best bid
        'best_ask_price': d['a'],               # STRING: best ask price
        'best_ask_qty': d['A'],                 # STRING: available quantity at best aks
        'open_price': d['o'],                   # STRING: opening price in the last 24h
        'high_price': d['h'],                   # STRING: highest price in the last 24h
        'low_price': d['l'],                    # STRING: lowest price in the last 24h
        'total_traded_base_asset': d['v'],      # STRING: traded volume in base asset
        'total_traded_quote_asset': d['q'],     # STRING: traded volume in quote asset
        'open_time': d['O'],                    # INT: opening timestamp of the 24h period
        'close_time': d['C'],                   # INT: closing timestamp of the 24h period
        'first_trade_id': d['F'],               # INT: ID of the first trade in the period
        'last_trade_id': d['L'],                # INT: ID of the last trade in the period
        'total_number_of_trades': d['n'],       # INT: total number of trades in the period
    }
    
    print(result)

async def ticker():
    connection = None
    try:
        connection = await client.websocket_streams.create_connection()

        stream = await connection.ticker(
            symbol="BTCUSDT",
        )
        stream.on("message", handle_message)
        

        await asyncio.sleep(5)
        await stream.unsubscribe()
    except Exception as e:
        logging.error(f"ticker() error: {e}")
    finally:
        if connection:
            await connection.close_connection(close_session=True)


if __name__ == "__main__":
    asyncio.run(ticker())