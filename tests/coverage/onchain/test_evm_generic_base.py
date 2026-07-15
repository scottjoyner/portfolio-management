from __future__ import annotations

from unittest import TestCase
from unittest.mock import MagicMock, patch

from onchain.chains.evm_generic.base import EVMChainAdapter


def _make_adapter(w3=None) -> EVMChainAdapter:
    return EVMChainAdapter(rpc_url="https://rpc", chain_id=1, _w3=w3)


class TestEVMChainAdapter(TestCase):
    def test_w3_property_existing(self):
        w3 = MagicMock()
        a = _make_adapter(w3=w3)
        self.assertIs(a.w3, w3)

    def test_w3_property_creates(self):
        fake = MagicMock()
        with patch("onchain.chains.evm_generic.base.Web3", return_value=fake) as mock_web3:
            a = _make_adapter(w3=None)
            got = a.w3
            self.assertIs(got, fake)
            self.assertIs(a._w3, fake)
            mock_web3.assert_called_once()

    def test_is_connected(self):
        w3 = MagicMock()
        w3.is_connected.return_value = True
        a = _make_adapter(w3=w3)
        self.assertTrue(a.is_connected())

    def test_get_block(self):
        w3 = MagicMock()
        w3.eth.get_block.return_value = {"number": 5}
        a = _make_adapter(w3=w3)
        self.assertEqual(a.get_block("latest", full_transactions=True), {"number": 5})

    def test_get_block_number(self):
        w3 = MagicMock()
        w3.eth.block_number = 42
        a = _make_adapter(w3=w3)
        self.assertEqual(a.get_block_number(), 42)

    def test_get_balance(self):
        w3 = MagicMock()
        w3.eth.get_balance.return_value = 100
        a = _make_adapter(w3=w3)
        self.assertEqual(a.get_balance("0x1111111111111111111111111111111111111111"), 100)

    def test_call_contract(self):
        w3 = MagicMock()
        fn = MagicMock()
        fn.return_value.call.return_value = 7
        w3.eth.contract.return_value.functions = MagicMock(balanceOf=fn)
        a = _make_adapter(w3=w3)
        result = a.call_contract("0x1111111111111111111111111111111111111111", [{"name": "balanceOf"}], "balanceOf", "0x1111111111111111111111111111111111111111")
        self.assertEqual(result, 7)

    def test_get_logs_with_address_and_topics(self):
        w3 = MagicMock()
        w3.eth.get_logs.return_value = [{"address": "0x1111111111111111111111111111111111111111"}]
        a = _make_adapter(w3=w3)
        logs = a.get_logs(1, 10, address="0x1111111111111111111111111111111111111111", topics=["0xt1"])
        self.assertEqual(logs, [{"address": "0x1111111111111111111111111111111111111111"}])

    def test_get_logs_without_address_and_topics(self):
        w3 = MagicMock()
        w3.eth.get_logs.return_value = []
        a = _make_adapter(w3=w3)
        logs = a.get_logs(1, 10)
        self.assertEqual(logs, [])
        # filter_params should not contain address/topics keys
        kwargs = w3.eth.get_logs.call_args.args[0]
        self.assertNotIn("address", kwargs)
        self.assertNotIn("topics", kwargs)

    def test_build_transaction_gas_and_price_provided(self):
        w3 = MagicMock()
        a = _make_adapter(w3=w3)
        tx = a.build_transaction("0x1111111111111111111111111111111111111111", value=5, data="0x", gas=100, gas_price=200)
        self.assertEqual(tx["gas"], 100)
        self.assertEqual(tx["gasPrice"], 200)
        w3.eth.estimate_gas.assert_not_called()
        w3.eth.gas_price.assert_not_called()

    def test_build_transaction_gas_and_price_default(self):
        w3 = MagicMock()
        w3.eth.estimate_gas.return_value = 21000
        w3.eth.gas_price = 30
        a = _make_adapter(w3=w3)
        tx = a.build_transaction("0x1111111111111111111111111111111111111111", value=5, data="0x")
        self.assertEqual(tx["gas"], 21000)
        self.assertEqual(tx["gasPrice"], 30)
        w3.eth.estimate_gas.assert_called_once()

    def test_send_transaction(self):
        w3 = MagicMock()
        w3.eth.send_raw_transaction.return_value = MagicMock(hex=MagicMock(return_value="0xhash"))
        a = _make_adapter(w3=w3)
        self.assertEqual(a.send_transaction(b"raw"), "0xhash")

    def test_wait_for_receipt(self):
        w3 = MagicMock()
        w3.eth.wait_for_transaction_receipt.return_value = {"status": 1}
        a = _make_adapter(w3=w3)
        self.assertEqual(a.wait_for_receipt("0xhash", timeout=10, poll_latency=0.1), {"status": 1})

    def test_estimate_gas(self):
        w3 = MagicMock()
        w3.eth.estimate_gas.return_value = 12345
        a = _make_adapter(w3=w3)
        self.assertEqual(a.estimate_gas("0x1111111111111111111111111111111111111111", value=1, data="0x"), 12345)

    def test_gas_price(self):
        w3 = MagicMock()
        w3.eth.gas_price = 99
        a = _make_adapter(w3=w3)
        self.assertEqual(a.gas_price(), 99)

    def test_close_with_w3(self):
        w3 = MagicMock()
        a = _make_adapter(w3=w3)
        a.close()
        self.assertIsNone(a._w3)

    def test_close_without_w3(self):
        a = _make_adapter(w3=None)
        a.close()  # no error
        self.assertIsNone(a._w3)
