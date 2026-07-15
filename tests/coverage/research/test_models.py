import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from _env import install_stubs  # noqa: E402

install_stubs()

from research import models as rm  # noqa: E402
from research.models import (  # noqa: E402
    HypothesisModel,
    MarketRegimeSnapshot,
    SignalCorrelationModel,
    BacktestResultModel,
    ResearchExperimentModel,
    create_research_engine,
)


class TestResearchModels(unittest.TestCase):
    def test_classes_imported(self):
        for cls in (
            HypothesisModel,
            MarketRegimeSnapshot,
            SignalCorrelationModel,
            BacktestResultModel,
            ResearchExperimentModel,
        ):
            self.assertTrue(hasattr(cls, "__tablename__"))

    def test_create_research_engine(self):
        eng = create_research_engine()
        self.assertIsNotNone(eng)
        # create_research_engine imports HypothesisGenerator internally
        self.assertTrue(hasattr(eng, "min_confidence"))

    def test_module_importable(self):
        self.assertIsNotNone(rm)


if __name__ == "__main__":
    unittest.main()
