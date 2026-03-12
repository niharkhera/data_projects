import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
import logging
from datetime import datetime, timedelta
from typing import Optional

# Import IndexConstructor from your existing script
from save_db import *
from index_construction import IndexConstructor

class StockIndexDashboard:
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize Stock Index Dashboard with index constructor.
        
        Args:
            db_path (str, optional): Path to SQLite database
        """
        # Setup logging
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        db_path = db_path or DB_PATH

        # Create index constructor
        self.index_constructor = IndexConstructor(db_path)
        
        # Optional: Add additional initialization logging
        self.logger.info(f"INFO: StockIndexDashboard initialized with database: {db_path}")
    
    def render_performance_chart(self, start_date: str = None, end_date: str = None):
        """
        Render line chart of index performance from index constructor
        
        Args:
            start_date (str, optional): Start date for performance tracking
            end_date (str, optional): End date for performance tracking
        """
        st.subheader('Index Performance Over Time')
        
        # Use default dates if not provided
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            self.logger.info(f"INFO: Rendering index performance between {start_date} to {end_date} ")
            # Create index performance using function
            performance_data = self.index_constructor.track_index_performance(start_date, end_date)
            # print(performance_data)

            
            if not performance_data.empty:
                performance_data['date'] = pd.to_datetime(performance_data['date'])
                
                # Create performance chart
                fig = px.line(
                    performance_data, 
                    x='date', 
                    y='daily_return', 
                    title='Equal Weighted Index Daily Returns'
                )
                st.plotly_chart(fig)
                
                # Display additional performance metrics
                st.subheader('Performance Summary')
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Return", f"{performance_data['daily_return'].sum():.2f}%")
                col2.metric("Average Daily Return", f"{performance_data['daily_return'].mean():.2f}%")
                col3.metric("Volatility", f"{performance_data['daily_return'].std():.2f}%")
            else:
                st.warning("No performance data available")
        
        except Exception as e:
            self.logger.error(f"ERROR: Error rendering performance chart: {e}")
            st.error("Failed to load performance chart")
    
    def render_index_composition(self, date: str = None, top_n: int = 100):
        """
        Render index composition for a specific date
        
        Args:
            date (str, optional): Date to show index composition
            top_n (int, optional): Number of top stocks to display
        """
        st.subheader('Index Composition')
        
        # Use yesterday's date if not provided
        if date is None:
            date =  (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        try:
            self.logger.info(f"INFO: Rendering index composition data for {date} ")
            # Retrieve index composition from database
            composition_df = self.index_constructor.db_manager.display_index_composition(date)
            # Create index composition using function
            if composition_df.empty or composition_df is None: 
                composition_df = self.index_constructor.construct_equal_weighted_index(date, top_n)
            
            if not composition_df.empty:
                # Bar chart of stock weights
                fig = px.bar(
                    composition_df, 
                    x='ticker', 
                    y='weight', 
                    title=f'Stock Weights in Index on {date}'
                )
                st.plotly_chart(fig)
                
                # Composition table
                st.dataframe(composition_df[['ticker', 'close_price', 'weight']])
                
                # Additional composition insights
                st.subheader('Composition Insights')
                col1, col2 = st.columns(2)
                col1.metric("Total Stocks", len(composition_df))
                col2.metric("Average Stock Price", f"${composition_df['close_price'].mean():.2f}")
            else:
                st.warning(f"No index composition data available for {date}")
        
        except Exception as e:
            self.logger.error(f"ERROR: Error rendering composition: {e}")
            st.error("Failed to load index composition")
    
    def render_index_changes(self, start_date: str = None, end_date: str = None):
        """
        Highlight days with index composition changes
        
        Args:
            start_date (str, optional): Start date for change tracking
            end_date (str, optional): End date for change tracking
        """
        st.subheader('Index Composition Changes')
        
        # Use default dates if not provided
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            # Retrieve index changes
            changes_df = self.index_constructor.detect_index_changes(start_date, end_date)
            
            if not changes_df.empty:
                st.dataframe(changes_df)
                st.metric("Number of Composition Changes", len(changes_df))
            else:
                st.info("No significant composition changes detected")
        
        except Exception as e:
            self.logger.error(f"ERROR: Error rendering composition changes: {e}")
            st.error("Failed to load composition changes")
    
    def run(self):
        """
        Main method to run the dashboard
        """
        try:
            st.title('Stock Index Performance Dashboard')
            
            # Date range selector
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input(
                    "Start Date", 
                    value=datetime.now() - timedelta(days=30)
                )
            with col2:
                end_date = st.date_input("End Date", value=datetime.now())
            
            # Convert dates to string format
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            # Performance Chart
            self.render_performance_chart(start_date_str, end_date_str)
            
            # Date Selection for Composition
            composition_date = st.date_input(
                'Select Date for Index Composition', 
                value=end_date
            )
            composition_date_str = composition_date.strftime('%Y-%m-%d')
            
            # # Index Composition
            self.render_index_composition(composition_date_str)
            
            # # Composition Changes
            self.render_index_changes(start_date_str, end_date_str)
        
        except Exception as e:
            self.logger.error(f"ERROR: Dashboard runtime error: {e}")
            st.error("An error occurred while running the dashboard")
        
        finally:
            # Ensure connection is closed
            self.index_constructor.close_db_connection()

def main():
    dashboard = StockIndexDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()

