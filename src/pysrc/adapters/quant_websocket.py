import asyncio
import json
import logging
import aiofiles
from pysrc.adapters.kraken.future.kraken_future_websocket_client import KrakenFutureWebsocketClient
from pysrc.util.types import Asset
import struct

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


def serialize_snapshot(time: float,
                       asset_code: int,
                       bids: list[tuple[float, float]],
                       asks: list[tuple[float, float]]) -> bytes:
    """
    Convert snapshot data to a binary format:
      [8 bytes time] [4 bytes asset_code]
      [4 bytes nb_bids] [ (8 bytes qty + 8 bytes price) * nb_bids ]
      [4 bytes nb_asks] [ (8 bytes qty + 8 bytes price) * nb_asks ]
    """
    data = bytearray()

    # 1) time (double)
    data.extend(struct.pack('>d', time))

    # 2) asset_code (int)
    data.extend(struct.pack('>i', asset_code))

    # 3) number of bids (int)
    data.extend(struct.pack('>i', len(bids)))

    # 4) each bid => qty(double), price(double)
    for (qty, price) in bids:
        data.extend(struct.pack('>dd', qty, price))

    # 5) number of asks (int)
    data.extend(struct.pack('>i', len(asks)))

    # 6) each ask => qty(double), price(double)
    for (qty, price) in asks:
        data.extend(struct.pack('>dd', qty, price))

    return data
async def poll_snapshots(client: KrakenFutureWebsocketClient, 
                         asset: Asset, 
                         snapshots_file, 
                         asset_code: int,
                         max_levels: int = 5):
    """
    Continuously polls snapshots from the WebSocket client,
    serializes them, and writes to a binary file.
    """
    while True:
        try:
            snapshots = client.poll_snapshots()
            if asset in snapshots:
                snapshot = snapshots[asset]

                # get top bids and asks             
                top_bids = snapshot.bids[:max_levels]
                top_asks = snapshot.asks[:max_levels]

                # Build the bytes to write
                bin_data = serialize_snapshot(
                    time=snapshot.time, 
                    asset_code=asset_code,  
                    bids=top_bids, 
                    asks=top_asks
                )

                # Write to the binary file
                await snapshots_file.write(bin_data)
                await snapshots_file.flush()

            await asyncio.sleep(0.1)  

        except Exception as e:
            _logger.error(f"Error in snapshot polling: {e}")
            await asyncio.sleep(0.1)

async def main():
    asset = Asset.QNT
    asset_code = 12 

    client = KrakenFutureWebsocketClient(subscribed_assets=[asset])
    client.start()
    _logger.info("WebSocket client started.")

    async with aiofiles.open('snapshots.bin', mode='ab') as snapshots_file:
        snapshot_task = asyncio.create_task(poll_snapshots(client, asset, snapshots_file, asset_code))

        try:
            await snapshot_task
        except asyncio.CancelledError:
            _logger.info("Polling task cancelled.")
        finally:
            snapshot_task.cancel()
            await asyncio.gather(snapshot_task, return_exceptions=True)
            if client.ws:
                await client.ws.close()
                _logger.info("WebSocket connection closed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        _logger.info("Program interrupted by user.")