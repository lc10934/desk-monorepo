import asyncio
import json
import logging
import aiofiles
from pysrc.adapters.kraken.future.kraken_future_websocket_client import KrakenFutureWebsocketClient
from pysrc.util.types import Asset

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

async def poll_snapshots(client: KrakenFutureWebsocketClient, asset: Asset, snapshots_file):
    buffer = []
    while True:
        try:
            snapshots = client.poll_snapshots()
            if asset in snapshots:
                snapshot = snapshots[asset]
                snapshot_row = {
                    'time': snapshot.time,  
                    'asset': asset.value,
                    'bids': json.dumps(snapshot.bids),
                    'asks': json.dumps(snapshot.asks)
                }
                # Convert dict to CSV row with formatted time
                snapshot_csv = f"{snapshot_row['time']:.3f},{snapshot_row['asset']},{snapshot_row['bids']},{snapshot_row['asks']}\n"
                buffer.append(snapshot_csv)
            
            if len(buffer) >= 10:  # Batch size of 10 snapshots (adjust as needed)
                await snapshots_file.write(''.join(buffer))
                await snapshots_file.flush()
                _logger.debug(f"{len(buffer)} snapshots written to snapshots.csv")
                buffer.clear()
            
            await asyncio.sleep(0.1)  # 100 ms
        except Exception as e:
            _logger.error(f"Error in snapshot polling: {e}")
            await asyncio.sleep(0.1)  

async def poll_trades(client: KrakenFutureWebsocketClient, trades_file):
    buffer = []
    while True:
        try:
            trades = client.poll_trades()
            for trade in trades:
                trade_row = {
                    'time': trade.time,
                    'product_id': trade.feedcode,
                    'n_trades': trade.n_trades,
                    'price': trade.price,
                    'quantity': trade.quantity,
                    'side': trade.side.value,
                    'market': trade.market.value
                }
                trade_csv = f"{trade_row['time']},{trade_row['product_id']},{trade_row['n_trades']},{trade_row['price']},{trade_row['quantity']},{trade_row['side']},{trade_row['market']}\n"
                buffer.append(trade_csv)
            
            if buffer:
                await trades_file.write(''.join(buffer))
                await trades_file.flush()
                _logger.debug(f"{len(buffer)} trade(s) written to trades.csv")
                buffer.clear()
            
            await asyncio.sleep(1) 
        except Exception as e:
            _logger.error(f"Error in trade polling: {e}")
            await asyncio.sleep(1)  

async def main():
    asset = Asset.QNT

    client = KrakenFutureWebsocketClient(subscribed_assets=[asset])

    client.start()
    _logger.info("WebSocket client started.")

    async with aiofiles.open('trades.csv', mode='w') as trades_file, \
               aiofiles.open('snapshots.csv', mode='w') as snapshots_file:

        trades_headers = ['time', 'product_id', 'n_trades', 'price', 'quantity', 'side', 'market']
        await trades_file.write(','.join(trades_headers) + '\n')

        snapshots_headers = ['time', 'asset', 'bids', 'asks']
        await snapshots_file.write(','.join(snapshots_headers) + '\n')

        snapshot_task = asyncio.create_task(poll_snapshots(client, asset, snapshots_file))
        trade_task = asyncio.create_task(poll_trades(client, trades_file))

        try:
            await asyncio.gather(snapshot_task, trade_task)
        except asyncio.CancelledError:
            _logger.info("Polling tasks cancelled.")
        finally:
            snapshot_task.cancel()
            trade_task.cancel()
            await asyncio.gather(snapshot_task, trade_task, return_exceptions=True)
            
            if client.ws:
                await client.ws.close()
                _logger.info("WebSocket connection closed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        _logger.info("Program interrupted by user.")
