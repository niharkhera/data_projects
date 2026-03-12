1) Understand the folder structure -> market_env -> project_structure.txt
2) Download softwares ->  market_env -> project_structure.txt/software_list.txt
3) Run fetch_data.py. This will create database and tables. (don't uncomment main functions yet)
4) import ticker_details data into sql (Download DbBrowser) (5 api req/min = 1000 req/3hr - Free Tier)
5) Change the stock_fetch_limit = 1000. Uncomment fetch_ohlc_stock_data in main functions & run. 
6) Check data in DbBrowser. It should populate
7) Run index_construction.py. Keep the dates between 2023-01-01 and 2023-01-30 for best experience
8) Hardcoded the dates. For construct_equal_weighted_index change the date starting from 2023-01-10. 
9) View the tables in DbBrowser. Check if data is populated (recommended: populate till 2023-01-28)
10) Run the export_data.py file. It will export two files under data/csv
11) Run (bash) streamlit run F:/path/to/file/market_env/src/stock_index_dashboard.py
12) Filter dates in browser from/between 2023-01-01 to 2023-01-30
13) Please connect with me on LinkedIn in case of any doubts https://www.linkedin.com/in/nihar-khera/ 


ARCHITECHTURE 

POLYGON API TICKER               -------> extract ->   DB   ->  TRANSFORM   ->   DB  ->  DASHBOARD/EXPORT
POLYGON API TICKER DETAILS V3     ------> python -> SQLite -> Pandas python -> SQLite -> stream lit  + plotly 
POLYGON API GROUPED DAILY (BARS) -------->                                           -> export csv

