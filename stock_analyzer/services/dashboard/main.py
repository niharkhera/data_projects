# services/dashboard/main.py

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

# Hardcoded for local testing. Will use Docker service name later.
API_BASE_URL = "http://localhost:8000"

st.set_page_config(page_title="Stock Index Dashboard", layout="wide")

st.title('📈 Modern Stock Index Dashboard (Microservice Edition)')
st.markdown("This dashboard retrieves data exclusively via REST API calls to the FastAPI gateway.")

tab_dash, tab_api_docs = st.tabs(["📊 View Dashboard", "📖 API Documentation"])

with tab_dash:
    with st.form("chart_generation_form"):
        st.subheader("Dashboard Parameters")
        view_strategy = st.selectbox("Select Strategy Type:", ["Market-Cap Weighted", "Equal Weighted"])
        comp_date = st.date_input("Index Composition Date", value=datetime.now() - timedelta(days=1))
        submitted = st.form_submit_button("Fetch Data from API", type="primary")

    if submitted:
        date_str = comp_date.strftime('%Y-%m-%d')
        
        # Make the REST HTTP Request to your FastAPI service
        st.info(f"Making GET request to {API_BASE_URL}/api/v1/composition...")
        try:
            response = requests.get(
                f"{API_BASE_URL}/api/v1/composition",
                params={"target_date": date_str, "index_type": view_strategy}
            )
            
            if response.status_code == 200:
                data = response.json()
                if data:
                    df = pd.DataFrame(data)
                    st.success(f"Successfully retrieved {len(df)} records via API!")
                    
                    st.subheader(f'🥧 {view_strategy} Composition (As of {date_str})')
                    fig = px.bar(df, x='ticker', y='weight', title='Stock Weights', color='weight', color_continuous_scale='viridis')
                    st.plotly_chart(fig, use_container_width=True)
                    
                    with st.expander("View Raw JSON Data"):
                        st.json(data)
                else:
                    st.warning(f"API returned empty list. No data generated for {date_str} yet.")
            else:
                st.error(f"API Error: HTTP {response.status_code} - {response.text}")
                
        except requests.exceptions.ConnectionError:
            st.error("Connection Error: Could not reach the FastAPI Gateway. Is it running on port 8000?")

with tab_api_docs:
    st.markdown("### Swagger UI")
    st.markdown(f"Interactive API documentation is automatically generated and hosted by FastAPI at:")
    st.code(f"{API_BASE_URL}/docs")