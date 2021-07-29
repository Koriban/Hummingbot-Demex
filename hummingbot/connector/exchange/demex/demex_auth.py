import hmac
import hashlib
from typing import Dict, Any
import mnemonic
from tradehub.wallet import Wallet

class DemexComAuth():
    """
    Auth class required by demex.com API
    Learn more at https://exchange-docs.demex.com/#digital-signature
    """
    def __init__(self, api_key: str, secret_key: str, mnemonic: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.mnemonic = mnemonic

    def generateWalletAddress(self) -> str:
        """
            Generates wallet address on the basis of mnemonic
        """
        # mnemonic = "insane once phone negative fly beyond wish video clog deal anger ladder"
        newWallet = Wallet(self.mnemonic,"mainnet")
        privateKey = newWallet.mnemonic_to_private_key(self.mnemonic)
        address = newWallet.private_key_to_address(privateKey)
        return address
    

    def generate_auth_dict(
        self,
        path_url: str,
        request_id: int,
        nonce: int,
        data: Dict[str, Any] = None
    ):
        """
        Generates authentication signature and return it in a dictionary along with other inputs
        :return: a dictionary of request info including the request signature
        """

        data = data or {}
        data['method'] = path_url
        data.update({'nonce': nonce, 'api_key': self.api_key, 'id': request_id})

        data_params = data.get('params', {})
        if not data_params:
            data['params'] = {}
        params = ''.join(
            f'{key}{data_params[key]}'
            for key in sorted(data_params)
        )

        payload = f"{path_url}{data['id']}" \
            f"{self.api_key}{params}{data['nonce']}"

        data['sig'] = hmac.new(
            self.secret_key.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        # print(data)
        return data

    def get_headers(self) -> Dict[str, Any]:
        """
        Generates authentication headers required by demex.com
        :return: a dictionary of auth headers
        """

        return {
            "Content-Type": 'application/json',
        }
