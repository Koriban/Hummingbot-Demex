#!/usr/bin/env python

import logging
import hummingbot.connector.exchange.demex.demex_constants as constants

from sqlalchemy.engine import RowProxy
from typing import (
    Optional,
    Dict,
    List, Any)
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import (
    OrderBookMessage, OrderBookMessageType
)
from hummingbot.connector.exchange.demex.demex_order_book_message import DemexComOrderBookMessage

_logger = None


class DemexComOrderBook(OrderBook):
    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _logger
        if _logger is None:
            _logger = logging.getLogger(__name__)
        return _logger

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None):
        """
        Convert json snapshot data into standard OrderBookMessage format
        :param msg: json snapshot data from live web socket stream
        :param timestamp: timestamp attached to incoming data
        :return: DemexComOrderBookMessage
        """

        if metadata:
            msg.update(metadata)

        return DemexComOrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def snapshot_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        """
        *used for backtesting
        Convert a row of snapshot data into standard OrderBookMessage format
        :param record: a row of snapshot data from the database
        :return: DemexComOrderBookMessage
        """
        return DemexComOrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=record.json,
            timestamp=record.timestamp
        )

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None):
        """
        Convert json diff data into standard OrderBookMessage format
        :param msg: json diff data from live web socket stream
        :param timestamp: timestamp attached to incoming data
        :return: DemexComOrderBookMessage
        """

        if metadata:
            msg.update(metadata)

        return DemexComOrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def diff_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        """
        *used for backtesting
        Convert a row of diff data into standard OrderBookMessage format
        :param record: a row of diff data from the database
        :return: DemexComOrderBookMessage
        """
        return DemexComOrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content=record.json,
            timestamp=record.timestamp
        )

    @classmethod
    def trade_message_from_exchange(cls,
                                    msg: Dict[str, Any],
                                    timestamp: Optional[float] = None,
                                    metadata: Optional[Dict] = None):
        """
        Convert a trade data into standard OrderBookMessage format
        :param record: a trade data from the database
        :return: DemexComOrderBookMessage
        """

        if metadata:
            msg.update(metadata)

        msg.update({
            "exchange_order_id": msg.get("d"),
            "trade_type": msg.get("s"),
            "price": msg.get("p"),
            "amount": msg.get("q"),
        })

        return DemexComOrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=msg,
            timestamp=timestamp
        )

    @classmethod
    def trade_message_from_db(cls, record: RowProxy, metadata: Optional[Dict] = None):
        """
        *used for backtesting
        Convert a row of trade data into standard OrderBookMessage format
        :param record: a row of trade data from the database
        :return: DemexComOrderBookMessage
        """
        return DemexComOrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content=record.json,
            timestamp=record.timestamp
        )

    @classmethod
    def from_snapshot(cls, snapshot: OrderBookMessage):
        raise NotImplementedError(constants.EXCHANGE_NAME + " order book needs to retain individual order data.")

    @classmethod
    def restore_from_snapshot_and_diffs(self, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
        raise NotImplementedError(constants.EXCHANGE_NAME + " order book needs to retain individual order data.")
