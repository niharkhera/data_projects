from fastapi import FastAPI, HTTPException, Query
from typing import List
from datetime import datetime, timedelta

# Import the shared Pydantic models
from shared.models import IndexPerformanceResponse, IndexCompositionResponse

# Initialize the FastAPI application
app = FastAPI(
    title="Stock Index Engine API",
    description="REST API gateway for the financial data pipeline.",
    version="0.3.0"
)

@app.get("/", tags=["Health"])
def health_check():
    """Simple health check endpoint to verify the API is running."""
    return {"status": "healthy", "service": "api-gateway"}

@app.get("/api/v1/performance", response_model=List[IndexPerformanceResponse], tags=["Analytics"])
def get_index_performance(
    index_type: str = Query("Equal Weighted", description="Type of index strategy"),
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format")
):
    """
    Retrieve historical performance data for a specific index.
    """
    try:
        # TODO: Replace this mock data with actual call to shared.db.py when we build it
        mock_data = [
            IndexPerformanceResponse(
                date=start_date,
                index_price=100.0,
                daily_return=0.0,
                index_type=index_type
            )
        ]
        return mock_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/composition", response_model=List[IndexCompositionResponse], tags=["Analytics"])
def get_index_composition(
    target_date: str = Query(..., description="Target date in YYYY-MM-DD format"),
    index_type: str = Query("Equal Weighted", description="Type of index strategy")
):
    """
    Retrieve the constituent weights of an index for a specific date.
    """
    try:
         # TODO: Replace this mock data with actual call to shared.db.py when we build it
        mock_data = [
            IndexCompositionResponse(
                ticker="AAPL",
                weight=0.05,
                market_cap=3000000000000,
                close_price=185.50
            )
        ]
        return mock_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))