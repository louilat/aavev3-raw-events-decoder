from web3 import Web3
import pandas as pd
from pandas import DataFrame


class AaveV3TokenTransferDecoder:
    def __init__(self):
        self.all_decoded_events: DataFrame = DataFrame()
        self.all_active_users: DataFrame = DataFrame()

    def decode_transfer_events(self, all_encoded_events: list) -> DataFrame:
        decoded_events = []
        for encoded_event in all_encoded_events:
            decoded_events.append(self._decode_transfer(eval(encoded_event)))

        transfers = pd.json_normalize(decoded_events)
        transfers = transfers[
            transfers["from"] != "0x0000000000000000000000000000000000000000"
        ]
        self.all_decoded_events = transfers
        return self.all_decoded_events

    def get_all_token_transfer_users(self) -> DataFrame:
        from_ = self.all_decoded_events["from"].tolist()
        to_ = self.all_decoded_events["to"].tolist()
        all_users = DataFrame({"active_user_address": from_ + to_}).drop_duplicates()
        self.all_active_users = all_users
        return all_users

    def _decode_transfer(self, transfer_encoded_event):
        blockNumber = transfer_encoded_event["blockNumber"]
        reserve = transfer_encoded_event["reserve"]
        from_ = Web3.to_checksum_address(transfer_encoded_event["topics"][1][-40:])
        to_ = Web3.to_checksum_address(transfer_encoded_event["topics"][2][-40:])

        event_data = transfer_encoded_event["data"][2:]
        amount = int(event_data[:64], 16)
        decoded_event = {
            "blockNumber": blockNumber,
            "reserve": reserve,
            "from": from_,
            "to": to_,
            "amount": amount,
        }
        return decoded_event
