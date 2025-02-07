import json
import pandas as pd
from pandas import DataFrame
from web3 import Web3


class AaveV3RawEventsDecoder:
    def __init__(self, contract_abi):
        self.contract_abi = contract_abi
        self.events_hex_signatures = dict()
        self.all_encoded_events_dict = dict()
        self.all_decoded_events_dict = dict()
        self.all_active_users = list()

    def get_events_signatures_as_hex(self, verbose: bool = False) -> dict:
        """
        Finds all contract's events from the ABI, and converts
        their signature as an hex string. Returns a array where the
        keys are the encoded signatures, and the values are the
        events names.

        Args:
            contract_abi (dict): The ABI of the smart contract
            verbose (bool): Whether to log execution details
        Returns:
            dict: The dictionary mapping the encoded signatures to
                the events' names.
        """
        hex_signatures_dict = dict()
        for abi in self.contract_abi:
            if abi["type"] == "event":
                event_name = abi["name"]
                event_inputs_list = []
                for input in abi["inputs"]:
                    event_inputs_list.append(input["type"])
                event_inputs_string = ",".join(event_inputs_list)
                event_signature = f"{event_name}({event_inputs_string})"
                event_signature_hex = Web3.to_hex(Web3.keccak(text=event_signature))
                hex_signatures_dict.update({event_signature_hex: event_name})
                if verbose:
                    print(abi["name"], ": ", event_signature)

        self.events_hex_signatures = hex_signatures_dict
        return hex_signatures_dict

    def classify_raw_events(self, raw_events: list) -> dict:
        all_encoded_events_dict = dict(
            {event_name: [] for event_name in self.events_hex_signatures.values()}
        )

        # Drop duplicates
        unique_raw_events = list(set(raw_events))

        # Classify events
        for encoded_event in unique_raw_events:
            encoded_event = json.loads(encoded_event)
            for event_hex_signature, event_name in self.events_hex_signatures.items():
                if event_hex_signature == encoded_event["topics"][0]:
                    all_encoded_events_dict[event_name].append(encoded_event)
                    continue

        self.all_encoded_events_dict = all_encoded_events_dict

        return all_encoded_events_dict

    def decode_raw_events(self) -> dict:
        decode_functions = {
            "Borrow": self._decode_borrow,
            "Supply": self._decode_supply,
            "Repay": self._decode_repay,
            "Withdraw": self._decode_withdraw,
            "LiquidationCall": self._decode_liquidation,
            "FlashLoan": self._decode_flashloan,
            "UserEModeSet": self._decode_emode,
            "ReserveUsedAsCollateralEnabled": self._decode_reserve_used_collateral_enabled,
            "ReserveUsedAsCollateralDisabled": self._decode_reserve_used_collateral_disabled,
            "BackUnbacked": self._decode_back_unbacked,
            "MintUnbacked": self._decode_mint_unbacked,
            "MintedToTreasury": self._decode_minted_to_treasury,
            "IsolationModeTotalDebtUpdated": self._decode_isolation_debt_updated,
            "ReserveDataUpdated": self._decode_reserve_data_updated,
        }

        all_decoded_events_dict = {
            event_name: [] for event_name in decode_functions.keys()
        }

        for event_name, events_list in all_decoded_events_dict.items():
            print(f"   --> Decoding {event_name}...")
            decoder = decode_functions[event_name]
            for encoded_event in self.all_encoded_events_dict[event_name]:
                events_list.append(decoder(encoded_event))

            all_decoded_events_dict[event_name] = pd.json_normalize(events_list)

        self.all_decoded_events_dict = all_decoded_events_dict
        return all_decoded_events_dict

    def get_all_active_users(self, verbose: bool = False) -> DataFrame:
        active_users_events = {
            "Borrow": ["onBehalfOf", "user"],
            "Supply": ["onBehalfOf", "user"],
            "Repay": ["user", "repayer"],
            "Withdraw": ["user", "to"],
            "LiquidationCall": ["user", "liquidator"],
            "FlashLoan": ["initiator", "target"],
            "UserEModeSet": ["user"],
            "ReserveUsedAsCollateralEnabled": ["user"],
            "ReserveUsedAsCollateralDisabled": ["user"],
            "BackUnbacked": ["backer"],
            "MintUnbacked": ["onBehalfOf", "user"],
        }
        all_active_users = list()
        for event_name, colnames in active_users_events.items():
            for colname in colnames:
                if len(self.all_decoded_events_dict[event_name]) > 0:
                    all_active_users.extend(
                        self.all_decoded_events_dict[event_name][colname]
                        .unique()
                        .tolist()
                    )
                    all_active_users = list(set(all_active_users))
        all_active_users = DataFrame({"active_user_address": all_active_users})
        self.all_active_users = all_active_users
        if verbose:
            print(f"   --> Found {len(all_active_users)} distinct active users")
        return all_active_users

    def _decode_borrow(self, borrow_encoded_event):
        blockNumber = borrow_encoded_event["blockNumber"]
        reserve = Web3.to_checksum_address(borrow_encoded_event["topics"][1][-40:])
        onBehalfOf = Web3.to_checksum_address(borrow_encoded_event["topics"][2][-40:])
        referralCode = int(borrow_encoded_event["topics"][3], 16)

        event_data = borrow_encoded_event["data"][2:]
        user = Web3.to_checksum_address(event_data[:64][-40:])
        amount = int(event_data[64 : 64 + 64], 16)
        interestRateMode = int(event_data[2 * 64 : 3 * 64], 16)
        borrowRate = int(event_data[3 * 64 :], 16)
        decoded_event = {
            "event": "Borrow",
            "blockNumber": blockNumber,
            "reserve": reserve,
            "onBehalfOf": onBehalfOf,
            "referralCode": referralCode,
            "user": user,
            "amount": amount,
            "interestRateMode": interestRateMode,
            "borrowRate": borrowRate,
        }
        return decoded_event

    def _decode_supply(self, supply_encoded_event):
        blockNumber = supply_encoded_event["blockNumber"]
        reserve = Web3.to_checksum_address(supply_encoded_event["topics"][1][-40:])
        onBehalfOf = Web3.to_checksum_address(supply_encoded_event["topics"][2][-40:])
        referralCode = int(supply_encoded_event["topics"][3], 16)

        event_data = supply_encoded_event["data"][2:]
        user = Web3.to_checksum_address(event_data[:64][-40:])
        amount = int(event_data[64 : 64 + 64], 16)
        decoded_event = {
            "event": "Supply",
            "blockNumber": blockNumber,
            "reserve": reserve,
            "onBehalfOf": onBehalfOf,
            "referralCode": referralCode,
            "user": user,
            "amount": amount,
        }
        return decoded_event

    def _decode_repay(self, repay_encoded_event):
        blockNumber = repay_encoded_event["blockNumber"]
        reserve = Web3.to_checksum_address(repay_encoded_event["topics"][1][-40:])
        user = Web3.to_checksum_address(repay_encoded_event["topics"][2][-40:])
        repayer = Web3.to_checksum_address(repay_encoded_event["topics"][3][-40:])

        event_data = repay_encoded_event["data"][2:]
        amount = int(event_data[:64], 16)
        useATokens = int(event_data[64 : 2 * 64], 16)
        decoded_event = {
            "event": "Repay",
            "blockNumber": blockNumber,
            "reserve": reserve,
            "user": user,
            "repayer": repayer,
            "amount": amount,
            "useATokens": useATokens,
        }
        return decoded_event

    def _decode_withdraw(self, withdraw_encoded_event):
        blockNumber = withdraw_encoded_event["blockNumber"]
        reserve = Web3.to_checksum_address(withdraw_encoded_event["topics"][1][-40:])
        user = Web3.to_checksum_address(withdraw_encoded_event["topics"][2][-40:])
        to = Web3.to_checksum_address(withdraw_encoded_event["topics"][3][-40:])

        event_data = withdraw_encoded_event["data"][2:]
        amount = int(event_data[:64], 16)
        decoded_event = {
            "event": "Withdraw",
            "blockNumber": blockNumber,
            "reserve": reserve,
            "user": user,
            "to": to,
            "amount": amount,
        }
        return decoded_event

    def _decode_liquidation(self, liquidation_encoded_event):
        blockNumber = liquidation_encoded_event["blockNumber"]
        collateralAsset = Web3.to_checksum_address(
            liquidation_encoded_event["topics"][1][-40:]
        )
        debtAsset = Web3.to_checksum_address(
            liquidation_encoded_event["topics"][2][-40:]
        )
        user = Web3.to_checksum_address(liquidation_encoded_event["topics"][3][-40:])

        event_data = liquidation_encoded_event["data"][2:]
        debtToCover = int(event_data[:64], 16)
        liquidatedCollateralAmount = int(event_data[64 : 2 * 64], 16)
        liquidator = Web3.to_checksum_address("0x" + event_data[2 * 64 : 3 * 64][-40:])
        receiveAToken = int(event_data[3 * 64 : 4 * 64], 16)
        decoded_event = {
            "event": "LiquidationCall",
            "blockNumber": blockNumber,
            "collateralAsset": collateralAsset,
            "debtAsset": debtAsset,
            "user": user,
            "debtToCover": debtToCover,
            "liquidatedCollateralAmount": liquidatedCollateralAmount,
            "liquidator": liquidator,
            "receiveAToken": receiveAToken,
        }
        return decoded_event

    def _decode_flashloan(self, flashloan_encoded_event):
        blockNumber = flashloan_encoded_event["blockNumber"]
        target = Web3.to_checksum_address(flashloan_encoded_event["topics"][1][-40:])
        asset = Web3.to_checksum_address(flashloan_encoded_event["topics"][2][-40:])
        referralCode = int(flashloan_encoded_event["topics"][3], 16)

        event_data = flashloan_encoded_event["data"][2:]
        initiator = Web3.to_checksum_address(event_data[:64][-40:])
        amount = int(event_data[64 : 2 * 64], 16)
        interestRateMode = int(event_data[2 * 64 : 3 * 64], 16)
        premium = int(event_data[3 * 64 : 4 * 64], 16)
        decoded_event = {
            "event": "FlashLoan",
            "blockNumber": blockNumber,
            "target": target,
            "asset": asset,
            "referralCode": referralCode,
            "initiator": initiator,
            "amount": amount,
            "interestRateMode": interestRateMode,
            "premium": premium,
        }
        return decoded_event

    def _decode_emode(self, emode_encoded_event):
        blockNumber = emode_encoded_event["blockNumber"]
        user = Web3.to_checksum_address(emode_encoded_event["topics"][1][-40:])

        event_data = emode_encoded_event["data"][2:]
        categoryId = int(event_data[:64], 16)
        decoded_event = {
            "event": "UserEModeSet",
            "blockNumber": blockNumber,
            "user": user,
            "categoryId": categoryId,
        }
        return decoded_event

    def _decode_reserve_used_collateral_enabled(self, reserve_collateral_encoded_event):
        blockNumber = reserve_collateral_encoded_event["blockNumber"]
        reserve = Web3.to_checksum_address(
            reserve_collateral_encoded_event["topics"][1][-40:]
        )
        user = Web3.to_checksum_address(
            reserve_collateral_encoded_event["topics"][2][-40:]
        )
        decoded_event = {
            "event": "ReserveUsedAsCollateralEnabled",
            "blockNumber": blockNumber,
            "reserve": reserve,
            "user": user,
        }
        return decoded_event

    def _decode_reserve_used_collateral_disabled(
        self, reserve_collateral_encoded_event
    ):
        blockNumber = reserve_collateral_encoded_event["blockNumber"]
        reserve = Web3.to_checksum_address(
            reserve_collateral_encoded_event["topics"][1][-40:]
        )
        user = Web3.to_checksum_address(
            reserve_collateral_encoded_event["topics"][2][-40:]
        )
        decoded_event = {
            "event": "ReserveUsedAsCollateralDisabled",
            "blockNumber": blockNumber,
            "reserve": reserve,
            "user": user,
        }
        return decoded_event

    def _decode_back_unbacked(self, back_unbacked_encoded_event):
        blockNumber = back_unbacked_encoded_event["blockNumber"]
        reserve = Web3.to_checksum_address(
            back_unbacked_encoded_event["topics"][1][-40:]
        )
        backer = Web3.to_checksum_address(
            back_unbacked_encoded_event["topics"][2][-40:]
        )

        event_data = back_unbacked_encoded_event["data"][2:]
        amount = int(event_data[:64], 16)
        fee = int(event_data[64:], 16)

        decoded_event = {
            "event": "BackUnbacked",
            "blockNumber": blockNumber,
            "reserve": reserve,
            "backer": backer,
            "amount": amount,
            "fee": fee,
        }
        return decoded_event

    def _decode_mint_unbacked(self, mint_unbacked_encoded_event):
        blockNumber = mint_unbacked_encoded_event["blockNumber"]
        reserve = Web3.to_checksum_address(
            mint_unbacked_encoded_event["topics"][1][-40:]
        )
        onBehalfOf = Web3.to_checksum_address(
            mint_unbacked_encoded_event["topics"][2][-40:]
        )
        referralCode = Web3.to_checksum_address(
            mint_unbacked_encoded_event["topics"][3][-40:]
        )

        event_data = mint_unbacked_encoded_event["data"][2:]
        user = Web3.to_checksum_address(event_data[:64][-40:])
        amount = int(event_data[64:], 16)

        decoded_event = {
            "event": "MintUnbacked",
            "blockNumber": blockNumber,
            "reserve": reserve,
            "onBehalfOf": onBehalfOf,
            "referralCode": referralCode,
            "user": user,
            "amount": amount,
        }
        return decoded_event

    def _decode_minted_to_treasury(self, treasury_encoded_event):
        blockNumber = treasury_encoded_event["blockNumber"]
        reserve = Web3.to_checksum_address(treasury_encoded_event["topics"][1][-40:])

        event_data = treasury_encoded_event["data"][2:]
        amountMinted = int(event_data[:64], 16)
        decoded_event = {
            "event": "MintedToTreasury",
            "blockNumber": blockNumber,
            "reserve": reserve,
            "amountMinted": amountMinted,
        }
        return decoded_event

    def _decode_isolation_debt_updated(self, isolation_debt_encoded_event):
        blockNumber = isolation_debt_encoded_event["blockNumber"]
        asset = Web3.to_checksum_address(
            isolation_debt_encoded_event["topics"][1][-40:]
        )

        event_data = isolation_debt_encoded_event["data"][2:]
        totalDebt = int(event_data[:64], 16)
        decoded_event = {
            "event": "IsolationModeTotalDebtUpdated",
            "blockNumber": blockNumber,
            "asset": asset,
            "totalDebt": totalDebt,
        }
        return decoded_event

    def _decode_reserve_data_updated(self, reserve_updated_encoded_event):
        blockNumber = reserve_updated_encoded_event["blockNumber"]
        reserve = Web3.to_checksum_address(
            reserve_updated_encoded_event["topics"][1][-40:]
        )

        event_data = reserve_updated_encoded_event["data"][2:]
        liquidityRate = int(event_data[:64], 16)
        stableBorrowRate = int(event_data[64 : 2 * 64], 16)
        variableBorrowRate = int(event_data[2 * 64 : 3 * 64], 16)
        liquidityIndex = int(event_data[3 * 64 : 4 * 64], 16)
        variableBorrowIndex = int(event_data[4 * 64 : 5 * 64], 16)
        decoded_event = {
            "event": "ReserveDataUpdated",
            "blockNumber": blockNumber,
            "reserve": reserve,
            "liquidityRate": liquidityRate,
            "stableBorrowRate": stableBorrowRate,
            "variableBorrowRate": variableBorrowRate,
            "liquidityIndex": liquidityIndex,
            "variableBorrowIndex": variableBorrowIndex,
        }
        return decoded_event
