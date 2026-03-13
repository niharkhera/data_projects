import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from typing import Optional

from save_db import StockDataManager
from index_construction import IndexConstructor
from logger_config import setup_logger
from export_data import DataExporter

class StockIndexDashboard:
    
    def __init__(self, db_path: Optional[str] = None):
        self.logger = setup_logger("DashboardUI")
        self.index_constructor = IndexConstructor(db_path)
        self.logger.info("Stock Index Dashboard application initialized.")
    
    def render_performance_chart(self, start_date: str, end_date: str, index_type: str):
        st.subheader(f'📈 {index_type} - Daily Returns')
        
        try:
            performance_data = self.index_constructor.track_index_performance(start_date, end_date, index_type)
            if not performance_data.empty:
                performance_data['date'] = pd.to_datetime(performance_data['date'])
                fig = px.line(performance_data, x='date', y='daily_return', title=f'Daily Index Returns (%)', markers=True)
                
                # FIXED: Deprecation warning resolved by using width='stretch'
                st.plotly_chart(fig, width='stretch')
                
                st.subheader('Performance Summary')
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Return", f"{performance_data['daily_return'].sum():.2f}%")
                col2.metric("Average Daily Return", f"{performance_data['daily_return'].mean():.2f}%")
                col3.metric("Volatility", f"{performance_data['daily_return'].std():.2f}%")
            else:
                st.warning(f"No performance data found for '{index_type}' between {start_date} and {end_date}.")
        except Exception as e:
            self.logger.error(f"Error rendering performance chart: {e}")
            st.error("Could not load the performance chart. Check logs.")
    
    def render_index_composition(self, date: str, top_n: int = 100, index_type: str = 'Equal Weighted'):
        st.subheader(f'🥧 {index_type} Composition (As of {date})')
        
        try:
            composition_df = self.index_constructor.db_manager.display_index_composition(date, index_type)
            
            if composition_df.empty: 
                if index_type == 'Market-Cap Weighted':
                    composition_df = self.index_constructor.construct_market_cap_weighted_index(date, top_n)
                else:
                    composition_df = self.index_constructor.construct_equal_weighted_index(date, top_n)
            
            if not composition_df.empty:
                fig = px.bar(composition_df, x='ticker', y='weight', title=f'Stock Weights', color='weight', color_continuous_scale='viridis')
                
                # FIXED: Deprecation warning resolved by using width='stretch'
                st.plotly_chart(fig, width='stretch')
                
                with st.expander("View Raw Composition Data Table"):
                    st.dataframe(composition_df[['ticker', 'close_price', 'weight', 'market_cap', 'index_type']])
            else:
                st.warning(f"No composition data available for {date}.")
        except Exception as e:
            self.logger.error(f"Error rendering composition chart: {e}")
            st.error("Could not load the index composition. Check logs.")
    
    def render_index_changes(self, start_date: str, end_date: str, index_type: str):
        st.subheader('🔄 Index Composition Changes')
        try:
            changes_df = self.index_constructor.detect_index_changes(start_date, end_date, index_type)
            if not changes_df.empty:
                st.dataframe(changes_df)
                st.metric("Composition Change Events (Days)", len(changes_df))
            else:
                st.info("No index composition changes detected in this window.")
        except Exception as e:
            self.logger.error(f"Error loading composition changes: {e}")
            st.error("Could not check for index changes.")

    def run(self):
        try:
            st.set_page_config(page_title="Stock Index Dashboard", layout="wide")
            st.title('📈 Modern Stock Index Dashboard')
            
            tab_dash, tab_fetch, tab_build = st.tabs(["📊 View Dashboard", "⬇️ Fetch Market Data", "🏗️ Select Index Strategy"])
            
            with tab_fetch:
                st.header("1. Fetch Company Profiles")
                st.info("Polygon Free Tier Limit: 5 per minute. Fetching 1,000 symbols will take ~3.3 hours.")
                meta_limit = st.slider("Max missing symbols to fetch this run:", 1, 1000, 50)
                
                if st.button("Fetch Missing Companies", type="primary"):
                    from fetch_data import StockDataFetcher
                    fetcher = StockDataFetcher()
                    existing_df = fetcher.db_manager.execute_query("SELECT ticker FROM ticker_details")
                    existing_tickers = set(existing_df['ticker'].tolist()) if not existing_df.empty else set()
                    
                    st.write("Comparing local DB against Polygon master roster...")
                    all_symbols = fetcher.fetch_all_stock_symbols(limit=1000) 
                    
                    if not all_symbols:
                        st.error("❌ Rate limit hit or API unavailable. Please wait 60 seconds.")
                    else:
                        missing_symbols = [s for s in all_symbols if s not in existing_tickers]
                        symbols_to_fetch = missing_symbols[:meta_limit]
                        
                        if not symbols_to_fetch:
                            st.success(f"DB is synced! All {len(all_symbols)} top tickers are present.")
                        else:
                            st.warning(f"Fetching {len(symbols_to_fetch)} new metadata profiles...")
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            import time
                            
                            for i, sym in enumerate(symbols_to_fetch):
                                status_text.text(f"Processing {sym} ({i+1}/{len(symbols_to_fetch)})...")
                                details = fetcher.fetch_stock_details(sym)
                                if details and details.get('results'):
                                    fetcher.db_manager.insert_ticker_details(details['results'])
                                progress_bar.progress((i + 1) / len(symbols_to_fetch))
                                if i < len(symbols_to_fetch) - 1:
                                    time.sleep(12.5) 
                            st.success("Metadata fetch complete!")
                    fetcher.close_db_connection()

                st.markdown("---")
                st.header("2. Fetch Historical Stock Prices")
                col1, col2 = st.columns(2)
                with col1: fetch_start = st.date_input("Start Date", value=datetime.now() - timedelta(days=5), key="f_start")
                with col2: fetch_end = st.date_input("End Date", value=datetime.now() - timedelta(days=1), key="f_end")
                
                if st.button("Download Prices", type="primary"):
                    from fetch_data import StockDataFetcher
                    fetcher = StockDataFetcher()
                    total_days = (fetch_end - fetch_start).days + 1
                    valid_days = [fetch_start + timedelta(days=x) for x in range(total_days) if (fetch_start + timedelta(days=x)).weekday() < 5]
                    
                    if not valid_days:
                        st.error("No valid trading days in selected range.")
                    else:
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        for i, curr_date in enumerate(valid_days):
                            date_str = curr_date.strftime('%Y-%m-%d')
                            status_text.text(f"Fetching OHLCV for {date_str}...")
                            prices = fetcher.fetch_ohlc_stock_data(date_str)
                            if prices: fetcher.db_manager.insert_stock_prices(prices)
                            progress_bar.progress((i + 1) / len(valid_days))
                        st.success("Price fetching complete!")
                    fetcher.close_db_connection()

            with tab_build:
                st.header("Select Index Strategy")
                st.markdown("Select a date range to generate the index for.")
                strategy = st.radio("Select Strategy:", ["Market-Cap Weighted", "Equal Weighted"])
                
                col1, col2 = st.columns(2)
                with col1: build_start = st.date_input("Start Date", value=datetime.now() - timedelta(days=1), key="b_start")
                with col2: build_end = st.date_input("End Date", value=datetime.now() - timedelta(days=1), key="b_end")
                
                if st.button("Generate Index Database", type="primary"):
                    total_days = (build_end - build_start).days + 1
                    valid_days = [build_start + timedelta(days=x) for x in range(total_days) if (build_start + timedelta(days=x)).weekday() < 5]
                    
                    if not valid_days:
                        st.error("No valid trading days selected.")
                    else:
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        for i, curr_date in enumerate(valid_days):
                            date_str = curr_date.strftime('%Y-%m-%d')
                            status_text.text(f"Calculating {strategy} weights for {date_str}...")
                            if "Market-Cap" in strategy:
                                self.index_constructor.construct_market_cap_weighted_index(date_str)
                            else:
                                self.index_constructor.construct_equal_weighted_index(date_str)
                            progress_bar.progress((i + 1) / len(valid_days))
                        st.success("Index calculation complete!")

            with tab_dash:
                with st.form("chart_generation_form"):
                    st.subheader("Dashboard Parameters")
                    view_strategy = st.selectbox("Select Strategy Type:", ["Market-Cap Weighted", "Equal Weighted"])
                    
                    st.markdown("##### Performance & Changes Window")
                    col1, col2 = st.columns(2)
                    with col1: chart_start = st.date_input("Start Date", value=datetime.now() - timedelta(days=30))
                    with col2: chart_end = st.date_input("End Date", value=datetime.now())
                    
                    st.markdown("##### Specific Day Composition Snapshot")
                    comp_date = st.date_input("Index Composition Date", value=datetime.now())
                    
                    submitted = st.form_submit_button("View Charts", type="primary")

                if submitted:
                    st.session_state['show_charts'] = True
                    st.session_state['chart_start'] = chart_start.strftime('%Y-%m-%d')
                    st.session_state['chart_end'] = chart_end.strftime('%Y-%m-%d')
                    st.session_state['comp_date'] = comp_date.strftime('%Y-%m-%d')
                    st.session_state['view_strategy'] = view_strategy

                if st.session_state.get('show_charts', False):
                    saved_start = st.session_state['chart_start']
                    saved_end = st.session_state['chart_end']
                    saved_comp_date = st.session_state['comp_date']
                    saved_strategy = st.session_state['view_strategy']
                    
                    self.render_performance_chart(saved_start, saved_end, saved_strategy)
                    st.markdown("---")
                    
                    self.render_index_composition(saved_comp_date, 100, saved_strategy)
                    st.markdown("---")
                    
                    self.render_index_changes(saved_start, saved_end, saved_strategy)
                    
                    st.markdown("---")
                    st.subheader("📥 Download Data to CSV")
                    
                    exporter = DataExporter(self.index_constructor.db_manager)
                    col_ex1, col_ex2 = st.columns(2)
                    
                    with col_ex1:
                        if st.button("Download Performance History"):
                            # Passes exact dates into the new exporter function format
                            path = exporter.export_performance(saved_strategy, saved_start, saved_end)
                            if path: st.success(f"File saved: {path}")
                            else: st.error("No performance data found to export.")
                                
                    with col_ex2:
                        if st.button(f"Download Composition for {saved_comp_date}"):
                            path = exporter.export_composition(saved_comp_date, saved_strategy)
                            if path: st.success(f"File saved: {path}")
                            else: st.error(f"No composition data found for {saved_comp_date}.")
                else:
                    st.info("👆 Configure parameters and click 'View Charts' to load.")
                    
        except Exception as e:
            self.logger.exception(f"Unhandled UI Exception: {e}")
            st.error("Dashboard encountered an error. See console logs.")
        finally:
            self.index_constructor.close_db_connection()

def main():
    dashboard = StockIndexDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()