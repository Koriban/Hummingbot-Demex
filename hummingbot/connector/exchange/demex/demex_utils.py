import math
from typing import Dict, List

from hummingbot.core.utils.tracking_nonce import get_tracking_nonce, get_tracking_nonce_low_res
from . import demex_constants as Constants

from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange


CENTRALIZED = True

EXAMPLE_PAIR = "ETH-USDT"

DEFAULT_FEES = [0.1, 0.1]

HBOT_BROKER_ID = "HBOT-"


# deeply merge two dictionaries
def merge_dicts(source: Dict, destination: Dict) -> Dict:
    for key, value in source.items():
        if isinstance(value, dict):
            # get node or create one
            node = destination.setdefault(key, {})
            merge_dicts(value, node)
        else:
            destination[key] = value

    return destination


# join paths
def join_paths(*paths: List[str]) -> str:
    return "/".join(paths)


# get timestamp in milliseconds
def get_ms_timestamp() -> int:
    return get_tracking_nonce_low_res()


# convert milliseconds timestamp to seconds
def ms_timestamp_to_s(ms: int) -> int:
    return math.floor(ms / 1e3)


# Request ID class
class RequestId:
    """
    Generate request ids
    """
    _request_id: int = 0

    @classmethod
    def generate_request_id(cls) -> int:
        return get_tracking_nonce()


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    return exchange_trading_pair.replace("_", "-")


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    return hb_trading_pair.replace("-", "_")


def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    side = "B" if is_buy else "S"
    return f"{HBOT_BROKER_ID}{side}-{trading_pair}-{get_tracking_nonce()}"


def get_api_reason(code: str) -> str:
    return Constants.API_REASONS.get(int(code), code)


KEYS = {
    "demex_com_api_key":
        ConfigVar(key="demex_com_api_key",
                  prompt="Enter your Demex.com API key >>> ",
                  required_if=using_exchange("demex_com"),
                  is_secure=True,
                  is_connect_key=False),
    "demex_com_secret_key":
        ConfigVar(key="demex_com_secret_key",
                  prompt="Enter your Demex.com secret key >>> ",
                  required_if=using_exchange("demex_com"),
                  is_secure=True,
                  is_connect_key=False),
    "demex_mnemonic":
        ConfigVar(key="demex_mnemonic",
                  prompt="Enter your demex mnemonic >>> ",
                  required_if=using_exchange("demex"),
                  is_secure=True,
                  is_connect_key=True)
}
