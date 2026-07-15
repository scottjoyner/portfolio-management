from __future__ import annotations

from unittest import TestCase

from onchain.models import SafetyState, TokenProfile
from onchain.security.token_safety.engine import TokenSafetyEngine


def _token(chain="base", address="0xTOK", state=SafetyState.TRUSTED, risk=0.1, decimals=18) -> TokenProfile:
    return TokenProfile(
        chain=chain,
        address=address,
        symbol="TKN",
        decimals=decimals,
        risk_score=risk,
        safety_state=state,
    )


class TestTokenSafetyEngine(TestCase):
    def test_register_and_classify_trusted(self):
        eng = TokenSafetyEngine()
        eng.register_token(_token())
        state, risk = eng.classify("base", "0xTOK")
        self.assertEqual(state, SafetyState.TRUSTED)
        self.assertEqual(risk, 0.1)

    def test_classify_unknown(self):
        eng = TokenSafetyEngine()
        state, risk = eng.classify("base", "0xMISSING")
        self.assertEqual(state, SafetyState.QUARANTINED)
        self.assertEqual(risk, 0.95)

    def test_classify_denied(self):
        eng = TokenSafetyEngine()
        eng.denylist.add(("base", "0xdeny".lower()))
        state, risk = eng.classify("base", "0xDENY")
        self.assertEqual(state, SafetyState.DENIED)
        self.assertEqual(risk, 1.0)

    def test_classify_address_lowercased(self):
        eng = TokenSafetyEngine()
        eng.register_token(_token(address="0xABC"))
        state, _ = eng.classify("base", "0xabc")
        self.assertEqual(state, SafetyState.TRUSTED)

    def test_validate_decimals_match(self):
        eng = TokenSafetyEngine()
        eng.register_token(_token(decimals=6))
        self.assertTrue(eng.validate_decimals("base", "0xTOK", 6))

    def test_validate_decimals_mismatch(self):
        eng = TokenSafetyEngine()
        eng.register_token(_token(decimals=6))
        self.assertFalse(eng.validate_decimals("base", "0xTOK", 18))

    def test_validate_decimals_unknown(self):
        eng = TokenSafetyEngine()
        self.assertFalse(eng.validate_decimals("base", "0xMISSING", 18))
