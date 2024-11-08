from asyncio import Queue
import json
import os
from typing import Optional, Any
import requests
from datetime import datetime, timedelta
import time
from pysrc.adapters.kraken.historical.updates.containers import (
    MBPBook,
    UpdateDelta,
    OrderEventResponse,
    OrderEventType,
)
from pysrc.adapters.kraken.historical.updates.utils import (
    str_to_order_event_type,
    str_to_order_side,
)
from pysrc.adapters.messages import TradeMessage
from pysrc.util.types import Market, OrderSide

import sys

from collections import deque
from threading import Event, Thread, Condition
import numpy as np
from pyzstd import CParameter, compress, decompress

import struct


class HistoricalUpdatesDataClient:
    def __init__(self, resource_path: str):
        self._resource_path = resource_path
        self._session = requests.Session()

        self._zstd_options = {CParameter.compressionLevel: 10}

    def _request(self, route: str, params: dict[str, Any]) -> Any:
        res = self._session.get(route, params=params)
        if res.status_code != 200:
            raise ValueError(f"Failed to get from '{route}', received {res.text}")

        return res.json()

    def _delta_from_order_event(self, e: dict) -> list[UpdateDelta]:
        event_json = e["event"]
        event_type_str = list(event_json.keys())[0]
        event_type = str_to_order_event_type(event_type_str)

        match event_type:
            case OrderEventType.PLACED | OrderEventType.CANCELLED:
                order_json = event_json[event_type_str]["order"]

                return [
                    UpdateDelta(
                        side=str_to_order_side(order_json["direction"]),
                        timestamp=e["timestamp"],
                        sign=1 if event_type == OrderEventType.PLACED else -1,
                        quantity=float(order_json["quantity"]),
                        price=float(order_json["limitPrice"]),
                    )
                ]
            case OrderEventType.UPDATED:
                new_order_json = event_json[event_type_str]["newOrder"]
                old_order_json = event_json[event_type_str]["oldOrder"]

                return [
                    UpdateDelta(
                        side=str_to_order_side(new_order_json["direction"]),
                        timestamp=e["timestamp"],
                        sign=1,
                        quantity=float(new_order_json["quantity"]),
                        price=float(new_order_json["limitPrice"]),
                    ),
                    UpdateDelta(
                        side=str_to_order_side(old_order_json["direction"]),
                        timestamp=e["timestamp"],
                        sign=-1,
                        quantity=float(old_order_json["quantity"]),
                        price=float(old_order_json["limitPrice"]),
                    ),
                ]
            case OrderEventType.REJECTED | OrderEventType.EDIT_REJECTED:
                return []
            case _:
                raise ValueError(f"Received malformed event {e}")

    def _get_order_events(
        self,
        asset: str,
        since: Optional[int] = None,
        before: Optional[int] = None,
        continuation_token: Optional[str] = None,
    ):
        route = f"https://futures.kraken.com/api/history/v3/market/{asset}/orders"

        params = {
            "sort": "asc",
            "since": since,
            "before": before,
            "continuation_token": continuation_token,
            "count": 2_000
        }

        res = self._request(route, params)

        # print(json.dumps(res["elements"], indent=4))
        deltas = []
        for e in res["elements"]:
            deltas.extend(self._delta_from_order_event(e))

        return OrderEventResponse(
            continuation_token=res.get("continuationToken"), deltas=deltas
        )
    
    def _delta_from_execution_event(self, e: dict) -> list[UpdateDelta]:
        event_json = e["event"]["Execution"]["execution"]

        return [
            UpdateDelta(
                side=OrderSide.BID,
                timestamp=event_json["timestamp"],
                sign=-1,
                quantity=float(event_json["quantity"]),
                price=float(event_json["price"]),
            ),
            UpdateDelta(
                side=OrderSide.ASK,
                timestamp=event_json["timestamp"],
                sign=-1,
                quantity=float(event_json["quantity"]),
                price=float(event_json["price"]),
            ),
        ]
    
    def _get_execution_events(
        self,
        asset: str,
        since: Optional[int] = None,
        before: Optional[int] = None,
        continuation_token: Optional[str] = None,
    ):
        route = f"https://futures.kraken.com/api/history/v3/market/{asset}/executions"

        params = {
            "sort": "asc",
            "since": since,
            "before": before,
            "continuation_token": continuation_token,
        }

        res = self._request(route, params)

        deltas = []
        for e in res["elements"]:
            deltas.extend(self._delta_from_execution_event(e))

        return OrderEventResponse(
            continuation_token=res.get("continuationToken"), deltas=deltas
        )
    
    def _queue_events_for_day(
            self,
            asset: str,
            day: datetime,
            should_get_order_events: bool,
            queue: deque,
            cond_var: Condition
    ):
        get_events_func = self._get_order_events if should_get_order_events else self._get_execution_events
        since_time = int(day.timestamp() * 1000)
        before_time = int((day + timedelta(days=1)).timestamp() * 1000)

        continuation_token = None
        while True:
            order_res = get_events_func(
                asset=asset,
                since=since_time,
                before=before_time,
                continuation_token=continuation_token
            )

            for d in order_res.deltas:
                queue.append(d)

            with cond_var:
                cond_var.notify()

            continuation_token = order_res.continuation_token
            if not continuation_token:
                break

        if should_get_order_events:
            self._done_getting_order_events = True
        else:    
            self._done_getting_exec_events = True

    def _serialize_mbp_book(
            self,
            file,
            timestamp: int
    ):
        snapshot = self._mbp_book.to_snapshot_message(timestamp)
        bids = np.array(snapshot.get_bids()).tobytes()
        asks = np.array(snapshot.get_asks()).tobytes()

        packed_metadata = struct.pack("QII", timestamp, len(bids), len(asks))

        file.write(packed_metadata + bids + asks)

    def _compute_updates_for_day(
            self,
            asset: str,
            day: datetime,
    ):
        file_path = os.path.join(
            self._resource_path,
            asset,
            day.strftime("%m_%d_%Y")
        )
        f = open(f"{file_path}_temp", "wb")

        order_deltas = deque()
        order_cond_var = Condition()
        order_thread = Thread(
            target=self._queue_events_for_day, args=(asset, day, True, order_deltas, order_cond_var)
        )
        order_thread.start()

        exec_deltas = deque()
        exec_cond_var = Condition()
        exec_thread = Thread(
            target=self._queue_events_for_day, args=(asset, day, False, exec_deltas, exec_cond_var)
        )
        exec_thread.start()

        cur_sec = int(day.timestamp())
        while True:
            if (not order_deltas and self._done_getting_order_events) or (not exec_deltas and self._done_getting_exec_events):
                break

            with order_cond_var:
                order_cond_var.wait_for(lambda: len(order_deltas))

            with exec_cond_var:
                exec_cond_var.wait_for(lambda: len(exec_deltas))

            if exec_deltas[0].timestamp < order_deltas[0].timestamp:
                delta = exec_deltas.popleft()
            else:
                delta = order_deltas.popleft()

            delta_time = delta.timestamp // 1000
            if delta_time != cur_sec:
                self._serialize_mbp_book(f, cur_sec + 1)
                cur_sec = delta_time

            self._mbp_book.apply_delta(delta)

        while not self._done_getting_order_events:
            with order_cond_var:
                order_cond_var.wait_for(lambda: len(order_deltas))

            delta = order_deltas.popleft()

            delta_time = delta.timestamp // 1000
            if delta_time != cur_sec:
                self._serialize_mbp_book(f, cur_sec + 1)
                cur_sec = delta_time

            self._mbp_book.apply_delta(delta)

        while not self._done_getting_exec_events:
            with exec_cond_var:
                exec_cond_var.wait_for(lambda: len(exec_deltas))

            delta = exec_deltas.popleft()

            delta_time = delta.timestamp // 1000
            if delta_time != cur_sec:
                self._serialize_mbp_book(f, cur_sec + 1)
                cur_sec = delta_time

            self._mbp_book.apply_delta(delta)

        order_thread.join()
        exec_thread.join()

        f.close()

        with open(f"{file_path}_temp", "rb") as temp_f:
            with open(file_path, "wb") as f:
                f.write(compress(temp_f.read(), level_or_option=self._zstd_options))

        os.remove(f"{file_path}_temp")

    def download_updates(
            self,
            asset: str,
            since: datetime,
            until: Optional[datetime] = None
    ):
        asset_path = os.path.join(self._resource_path, asset)
        if not os.path.exists(asset_path):
            os.mkdir(asset_path)

        if not until:
            until = datetime.now()

        while since.timestamp() < until.timestamp():
            self._mbp_book = MBPBook(
                feedcode=asset,
                market=Market.KRAKEN_USD_FUTURE
            )

            self._done_getting_order_events = False
            self._done_getting_exec_events = False

            self._compute_updates_for_day(asset, since)
            since += timedelta(days=1)
