from onchain.contracts.registry.service import ContractRegistry
from onchain.models import ContractProfile, SafetyState
from onchain.wallets.policy_engine.engine import WalletPolicy, WalletPolicyEngine


def test_contract_registry_selector_and_denylist() -> None:
    reg = ContractRegistry()
    reg.register(
        ContractProfile(
            chain="base",
            address="0xabc",
            protocol="uni",
            codehash="0x123456789",
            verified_abi=True,
            selectors_allowlist={"0xdeadbeef"},
            safety_state=SafetyState.TRUSTED,
        )
    )
    assert reg.is_allowed("base", "0xabc", "0xdeadbeef")
    assert not reg.is_allowed("base", "0xabc", "0x00000000")
    reg.deny("base", "0xabc")
    assert not reg.is_allowed("base", "0xabc")


def test_wallet_policy_caps() -> None:
    engine = WalletPolicyEngine()
    engine.register_policy(WalletPolicy(wallet="hot", daily_spend_limit=1000, per_contract_cap=500, per_token_cap=700, allowance_cap=300, bridge_limit=200))
    ok, _ = engine.approve_spend("hot", 100, 100, 100, 50)
    assert ok
    ok, reason = engine.approve_spend("hot", 950, 100, 100, 50)
    assert not ok
    assert reason == "daily spend limit exceeded"
