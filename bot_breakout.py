"""bots/ — All 7 trading bot implementations."""
from .bot_funding_arb      import FundingArbBot      # Priority 2 — deploy first
from .bot_trend_follower   import TrendFollowerBot    # Priority 3
from .bot_breakout         import BreakoutBot         # Priority 4
from .bot_mean_reversion   import MeanReversionBot    # Priority 5
from .bot_scalper          import ScalperBot          # Priority 6
from .bot_market_maker     import MarketMakerBot      # Priority 7
from .bot_multi_momentum   import MultiMomentumBot    # Priority 8

__all__ = [
    "FundingArbBot",
    "TrendFollowerBot",
    "BreakoutBot",
    "MeanReversionBot",
    "ScalperBot",
    "MarketMakerBot",
    "MultiMomentumBot",
]
