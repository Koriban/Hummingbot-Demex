#!/usr/bin/env python
import asyncio
import logging

import time
import aiohttp
import pandas as pd
import hummingbot.connector.exchange.switcheo.switcheo_constants as constants

import nest_asyncio

from datetime import datetime
from tradehub.websocket_client import DemexWebsocket

from typing import Optional, List, Dict, Any
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.logger import HummingbotLogger
from . import switcheo_utils
from .switcheo_active_order_tracker import DemexActiveOrderTracker
from .switcheo_order_book import DemexOrderBook
from .switcheo_websocket import DemexWebsocket
from .switcheo_utils import ms_timestamp_to_s


class SwitcheoAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MAX_RETRIES = 20
    MESSAGE_TIMEOUT = 30.0
    SNAPSHOT_TIMEOUT = 10.0
    nest_asyncio.apply()

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pairs: List[str] = None):
        super().__init__(trading_pairs)
        self._trading_pairs: List[str] = trading_pairs
        self._snapshot_msg: Dict[str, any] = {}

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        result = {}
        async with aiohttp.ClientSession() as client:
            resp = await client.get(f"{constants.REST_URL}/get_market_stats")
            resp_json = await resp.json()
            for t_pair in trading_pairs:
                print(t_pair)
                # print(o)
                temp = t_pair.lower()
                print(temp)
                for o in resp_json:
                    print(o)
                    if o["market"] == switcheo_utils.convert_to_exchange_trading_pair(temp):
                         print("true - " , o["market"])
                last_trade = [o["last_price"] for o in resp_json if o["market"] ==
                              switcheo_utils.convert_to_exchange_trading_pair(temp)]
                print("last_trade - ", last_trade)
                if last_trade and last_trade[0] is not None:
                    result[t_pair] = last_trade[0]
            print("tested - ",result)
        return result

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        async with aiohttp.ClientSession() as client:
            async with client.get(f"{constants.REST_URL}/get_markets", timeout=10) as response:
                if response.status == 200:
                    from hummingbot.connector.exchange.switcheo.switcheo_utils import \
                        convert_from_exchange_trading_pair
                    try:
                        data: Dict[str, Any] = await response.json()
                        return [convert_from_exchange_trading_pair(item["name"]) for item in data]
                    except Exception:
                        pass
                        # Do nothing if the request fails -- there will be no autocomplete for kucoin trading pairs
                return []

    @staticmethod
    async def get_order_book_data(trading_pair: str) -> Dict[str, any]:
        """
        Get whole orderbook
        """
        async with aiohttp.ClientSession() as client:
            orderbook_response = await client.get(
                f"{constants.REST_URL}/get_orderbook?limit=150&market="
                f"{switcheo_utils.convert_to_exchange_trading_pair(trading_pair)}"
            )

            if orderbook_response.status != 200:
                raise IOError(
                    f"Error fetching OrderBook for {trading_pair} at {constants.EXCHANGE_NAME}. "
                    f"HTTP status is {orderbook_response.status}."
                )

            orderbook_data: List[Dict[str, Any]] = await safe_gather(orderbook_response.json())
            # print(orderbook_data)
            asks = []
            bids = []
            for key,val in orderbook_data[0].items():
                if key == "asks":
                    for temp in val:
                        asks.append(list(temp.values()))
                elif key == "bids":
                    for temp in val:
                        bids.append(list(temp.values()))

            orderbook_data[0]["asks"] = asks
            orderbook_data[0]["bids"] = bids
            # orderbook_data = orderbook_data[0]["result"]["data"][0]
            orderbook_data = orderbook_data[0]
            # print("------------------------")
            # print(orderbook_data)
            # print("------------------------")

        return orderbook_data 

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        snapshot: Dict[str, Any] = await self.get_order_book_data(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = DemexOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        order_book = self.order_book_create_function()
        active_order_tracker: DemexActiveOrderTracker = DemexActiveOrderTracker()
        bids, asks = active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
        order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)
        # print("Hello from Switcheo Connector")
        return order_book

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """ 
        Listen for trades using websocket trade channel
        """
        while True:
            try:
                # response = {}
                # async def on_connect():
                #     print("I am connect to demex websocket!")
                #     # await demex.subscribe_books("orderbook", "swth_eth1")
                #     await demex.get_recent_trades('recent_trades', switcheo_utils.convert_to_exchange_trading_pair(self._trading_pairs[0]))

                # async def on_receive_message(message: dict):
                #     print("I received a message")
                #     # print(message)
                #     response = message

                # demex: DemexWebsocket = DemexWebsocket(constants.WSS_PUBLIC_URL)
                # asyncio.get_event_loop().run_until_complete(demex.connect(on_receive_message, on_connect))


                ws = DemexWebsocket()
                await ws.connect()

                await ws.subscribe(list(map(
                    lambda pair: f"recent_trades.{switcheo_utils.convert_to_exchange_trading_pair(pair)}",
                    self._trading_pairs
                )))

                async for response in ws.on_message():
                    if response.get("result") is None:
                        continue

                for trade in response["result"]:
                    utc_time = datetime.strptime(trade["block_created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
                    trade["block_created_at"] = epoch_time
                    trade: Dict[Any] = trade
                    trade_timestamp: int = ms_timestamp_to_s(trade["block_created_at"])
                    trade_msg: OrderBookMessage = DemexOrderBook.trade_message_from_exchange(
                        trade,
                        trade_timestamp,
                        metadata={"trading_pair": switcheo_utils.convert_from_exchange_trading_pair(trade["market"])}
                    )
                    output.put_nowait(trade_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
            finally:
                await ws.disconnect()

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for orderbook diffs using websocket book channel
        """
        while True:
            try:
                ws = DemexWebsocket()
                await ws.connect()

                await ws.subscribe(list(map(
                    lambda pair: f"books.{switcheo_utils.convert_to_exchange_trading_pair(pair)}",
                    self._trading_pairs
                )))

                # async def on_connect():
                #     print("I am connect to demex websocket!")
                #     # await demex.subscribe_books("orderbook", "swth_eth1")
                #     list(map(
                #         lambda pair: f"book.{switcheo_utils.convert_to_exchange_trading_pair(pair)}.150",
                #         self._trading_pairs
                #     ))
                #     # await demex.get_recent_trades('recent_trades', switcheo_utils.convert_to_exchange_trading_pair(self._trading_pairs[0]))
                #     # await demex.subscribe_books("offerbook",  switcheo_utils.convert_to_exchange_trading_pair(self._trading_pairs[0]))

                # async def on_receive_message(message: dict):
                #     epoch = time.time()
                #     print("I received a message")
                #     # print(message)
                #     asks = []
                #     bids = []
                #     res = {}

                #     if len(message["result"]) > 1:
                #         for data in message["result"]: 
                #             market = data["market"]
                #             if data["side"] == "buy":
                #                 bids.append([data["price"],data["amount"]])
                #             elif data["side"] == "sell":
                #                 asks.append([data["price"],data["amount"]])
                #         # print(asks[0], "\n", bids[0])
                        
                #         res["asks"] = asks[0]
                #         res["bids"] = bids[0]
                #     # if response.get("result") is None:
                #     #     continue

                #     order_book_data = res
                #     timestamp: int = ms_timestamp_to_s(epoch)
                #     # timestamp: int = ms_timestamp_to_s(order_book_data["t"])

                #     # data in this channel is not order book diff but the entire order book (up to depth 150).
                #     # so we need to convert it into a order book snapshot.
                #     # Demex does not offer order book diff ws updates.
                #     orderbook_msg: OrderBookMessage = DemexOrderBook.snapshot_message_from_exchange(
                #         order_book_data,
                #         timestamp,
                #         metadata={"trading_pair": switcheo_utils.convert_from_exchange_trading_pair(
                #             market)}
                #     )
                #     print("Hello from Switcheo Connector 2")
                #     output.put_nowait(orderbook_msg)

                # demex: DemexWebsocket = DemexWebsocket(constants.WSS_PUBLIC_URL)
                # asyncio.get_event_loop().run_until_complete(demex.connect(on_receive_message, on_connect))

                async for response in ws.on_message():
                    if response.get("result") is None:
                        continue
                    
                    order_book_data = response["result"]["data"][0]
                    timestamp: int = ms_timestamp_to_s(order_book_data["t"])
                    # data in this channel is not order book diff but the entire order book (up to depth 150).
                    # so we need to convert it into a order book snapshot.
                    # Demex does not offer order book diff ws updates.
                    orderbook_msg: OrderBookMessage = DemexOrderBook.snapshot_message_from_exchange(
                        order_book_data,
                        timestamp,
                        metadata={"trading_pair": switcheo_utils.convert_from_exchange_trading_pair(
                            response["result"]["instrument_name"])}
                    )
                    # print("Hello from Switcheo Connector 2")
                    output.put_nowait(orderbook_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unexpected error with WebSocket connection.",
                    exc_info=True,
                    app_warning_msg="Unexpected error with WebSocket connection. Retrying in 30 seconds. "
                                    "Check network connection."
                )
                await asyncio.sleep(30.0)
            finally:
                await ws.disconnect()

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for orderbook snapshots by fetching orderbook
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: Dict[str, any] = await self.get_order_book_data(trading_pair)
                        # snapshot_timestamp: int = ms_timestamp_to_s(snapshot["t"])
                        snapshot_timestamp: int = ms_timestamp_to_s(time.time()) # set the timestamp static for now
                        snapshot_msg: OrderBookMessage = DemexOrderBook.snapshot_message_from_exchange(
                            snapshot,
                            snapshot_timestamp,
                            metadata={"trading_pair": trading_pair}
                        )
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                        # Be careful not to go above API rate limits.
                        await asyncio.sleep(5.0)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self.logger().network(
                            "Unexpected error with WebSocket connection.",
                            exc_info=True,
                            app_warning_msg="Unexpected error with WebSocket connection. Retrying in 5 seconds. "
                                            "Check network connection."
                        )
                        await asyncio.sleep(5.0)
                this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                delta: float = next_hour.timestamp() - time.time()
                await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
