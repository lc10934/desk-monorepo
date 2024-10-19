import requests
from pysrc.util.conversions import enum_to_string, string_to_enum
from pysrc.adapters.kraken.spot.containers import (
    SystemStatus,
    AssetInfo,
    AssetPairInfo,
    OHLCData,
    AssetStatus,
    OHLCTick,
    SpreadMessage,
    TradeableAssetPairParam,
)
from typing import Optional, Dict
from pysrc.util.types import Asset, Market, OrderSide
from pysrc.adapters.messages import TradeMessage, SnapshotMessage

KRAKEN_API_LIVE_BASE_URL =  "https://api.kraken.com/0/public/"
KRAKEN_API_TESTNET_BASE_URL = ""

class KrakenClient:
    def __init__(
        self, use_live_api: bool = True):
        self._base_url: str = (
            KRAKEN_API_LIVE_BASE_URL if use_live_api else KRAKEN_API_TESTNET_BASE_URL
        )
        self._kraken_to_asset = {"XXBTZUSD": Asset.BTC}  
        self._asset_to_kraken = {Asset.BTC: "XXBTZUSD"}

    def _get(self, endpoint: str, params: Optional[Dict[str, str]] = None) -> dict:
        url = f"{self._base_url}{endpoint}"

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            res = response.json()

            if res["error"]:
                raise ValueError(f"API Error: {response['error']}")

            return res

        except requests.exceptions.RequestException as e:
            raise ValueError(f"Failed to fetch data from {endpoint}: {e}")

    def get_system_status(self) -> SystemStatus:
        route = "SystemStatus"
        response = self._get(route)
        return string_to_enum(SystemStatus, response["result"]["status"].upper())

    def get_asset_info(self, asset: Asset, aclass: str = "currency") -> AssetInfo:
        route = "Assets"
        params = {"asset": asset.name, "aclass": aclass}

        response = self._get(route, params)
        asset_data = response["result"][asset.name]
        return AssetInfo(
            asset=asset,
            asset_name=asset_data["altname"],
            altname=asset_data["altname"],
            decimals=asset_data["decimals"],
            collateral_value=asset_data.get("collateral_value"),
            status=AssetStatus[asset_data["status"].replace(" ", "_").upper()],
        )

    def get_tradeable_asset_pairs(
        self, pairs: list[Asset], info: TradeableAssetPairParam = TradeableAssetPairParam.INFO
    ) -> Dict[str, AssetPairInfo]:
        pair_param = ",".join(
            [f"{enum_to_string(asset)}/USDT" for asset in pairs]
        )

        route = "AssetPairs"
        params = {
            "pair": pair_param,
            "info": info.name.lower(),
        }

        response = self._get(route, params=params)

        asset_pairs = {}
        for pair_name, pair_data in response["result"].items():
            print(pair_name)
            asset_pairs[pair_name] = AssetPairInfo(
                altname=pair_data["altname"],
                wsname=pair_data.get("wsname"),
                aclass_base=pair_data["aclass_base"],
                base=pair_data["base"],
                aclass_quote=pair_data["aclass_quote"],
                quote=pair_data["quote"],
            )
        return asset_pairs

    def get_ohlc_data(
        self, asset: Asset, interval: int = 1, since: Optional[int] = None
    ) -> OHLCData:

        pair_param = f"{enum_to_string(asset)}USD"

        route = "OHLC"
        params = {"pair": pair_param, "interval": interval, "since": since}

        response = self._get(route, params=params)

        last_timestamp = response["result"].get("last", None)

        pair_name = list(response["result"].keys())[0]
        tick_data = response["result"][self._asset_to_kraken[asset]]

        ohlc_ticks = [
            OHLCTick(
                time=tick[0],
                open=tick[1],
                high=tick[2],
                low=tick[3],
                close=tick[4],
                vwap=tick[5],
                volume=tick[6],
                count=tick[7],
            )
            for tick in tick_data
        ]

        return OHLCData(
            asset=self._kraken_to_asset[pair_name],
            ticks=ohlc_ticks,
            last=last_timestamp,
        )

    def get_order_book(self, asset: Asset, count: int = 100) -> SnapshotMessage:

        pair_param = f"{enum_to_string(asset)}USD"

        route = "Depth"
        params = {"pair": pair_param, "count": count}

        response = self._get(route, params=params)
        order_book_data = response["result"][self._asset_to_kraken[asset]]

        bids = [(price, volume) for price, volume, _ in order_book_data["bids"]]
        asks = [(price, volume) for price, volume, _ in order_book_data["asks"]]
        timestamp = int(order_book_data["bids"][0][2])
        feedcode = pair_param

        return SnapshotMessage(
            time=timestamp,
            feedcode=feedcode,
            bids=bids,
            asks=asks,
            market=Market.KRAKEN_SPOT,
        )

    def get_recent_trades(
        self, asset: Asset, since: int = None, count: int = 1000
    ) -> list[TradeMessage]:

        pair_param = f"{enum_to_string(asset)}USD"

        route = "Trades"
        params = {"pair": pair_param, "count": count, "since": str(since)}

        response = self._get(route, params=params)

        trade_data = response["result"][self._asset_to_kraken[asset]]
        trades = []

        for trade in trade_data:
            price = float(trade[0])
            quantity = float(trade[1])
            timestamp = int(trade[2])
            side = OrderSide.BID if trade[3] == "b" else OrderSide.ASK

            trades.append(
                TradeMessage(
                    time=timestamp,
                    feedcode=pair_param,
                    n_trades=1,
                    price=price,
                    quantity=quantity,
                    side=side,
                    market=Market.KRAKEN_SPOT,
                )
            )

        return trades

    def get_recent_spreads(
        self, asset: Asset, since: int = None
    ) -> list[SpreadMessage]:
        pair_param = f"{enum_to_string(asset)}USD"

        route = "Spread"
        params = {"pair": pair_param, "since" : str(since)}

        response = self._get(route, params=params)

        spread_data = response["result"][self._asset_to_kraken[asset]]
        spreads = []

        for spread in spread_data:
            timestamp = int(spread[0])
            bid = float(spread[1])
            ask = float(spread[2])

            spreads.append(
                SpreadMessage(time=timestamp, feedcode=pair_param, bid=bid, ask=ask)
            )

        return spreads
