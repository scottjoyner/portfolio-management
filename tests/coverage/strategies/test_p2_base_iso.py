import importlib.util, os, sys
p = os.path.abspath("trading_system/strategies/base.py")
spec = importlib.util.spec_from_file_location("strat_base_iso", p)
M = importlib.util.module_from_spec(spec)
sys.modules["strat_base_iso"] = M
spec.loader.exec_module(M)

def test_z():
    assert M.compute_z_score([], 2) == []
    assert M.compute_z_score([1.0], 2) == []
    assert M.compute_z_score([10.0,10.0,10.0,10.0]) == [0.0,0.0,0.0,0.0]
    M.compute_z_score([1.0,2.0,3.0,4.0,5.0])
