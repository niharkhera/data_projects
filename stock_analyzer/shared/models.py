from pydantic import BaseModel, Field
from typing import Optional

class IndexPerformanceResponse(BaseModel):
    """Schema for returning index performance data."""
    date: str = Field(..., description="The trading date (YYYY-MM-DD)")
    index_price: float = Field(..., description="Calculated price of the index")
    daily_return: float = Field(..., description="Percentage return from previous day")
    index_type: str = Field(..., description="e.g., 'Equal Weighted' or 'Market-Cap Weighted'")

class IndexCompositionResponse(BaseModel):
    """Schema for returning index composition weights."""
    ticker: str = Field(..., description="Stock ticker symbol")
    weight: float = Field(..., description="Calculated weight of the stock in the index")
    market_cap: Optional[float] = Field(None, description="Company market capitalization")
    close_price: float = Field(..., description="Closing price on the given date")