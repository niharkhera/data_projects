# Stock Index Dashboard

A Python-based dashboard application for constructing, tracking, and visualizing stock market indices. This project provides tools for creating equal-weighted stock indices, monitoring their performance, and analyzing composition changes over time.

## Features

- **Index Construction**: Creates equal-weighted indices from top market cap stocks
- **Performance Tracking**: Monitors and visualizes index performance over time
- **Composition Analysis**: Detects and tracks changes in index composition
- **Interactive Dashboard**: Built with Streamlit for real-time visualization
- **Data Export**: Functionality to export index data to CSV files
  

## Prerequisites

- Python 3.6.8
- IDE
- git bash (git version 2.35.2.windows.1)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/stock-index-dashboard.git
cd stock-index-dashboard
```

2. Create and activate a virtual environment:
```bash
python -m venv sm_venv
source sm_venv/Scripts/activate  # On Windows: sm_venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
pip install --upgrade pip setuptools wheel
pip install pywinpty
```

4. Create a `.env` file in the project root and add your Polygon.io API key:
```
POLYGON_API_KEY=your_api_key_here
```


## Project Structure

```

your-folder-on-desktop/
├── market_env/
│   ├── data/
│   │   ├── csv/
│   │   │   ├── index_composition.csv
│   │   │   └── index_performance.csv
│   │   └── backup_csv_for_db/
│   ├── db/
│   │   └── stock_data.db
│   ├── docs/
│   │   ├── requirements.txt
│   │   └── software.txt
│   ├── Include/
│   ├── Lib/
│   ├── screenshots/
│   ├── scripts/
│   ├── src/
│   │   ├── export_data.py
│   │   ├── fetch_data.py
│   │   ├── index_construction.py
│   │   ├── save_db.py
│   │   ├── stock_index_dashboard.py
│   │   └── .env
│   └── pyvenv.cfg/
└── README.md

```

## Usage

1. Start the Streamlit dashboard:
```bash
streamlit run src/dashboard.py
```

2. Export index data to CSV:
```bash
python src/export_data.py
```

## Key Components

### StockDataManager (save_db.py)
- Handles database operations
- Manages tables for stock data, index composition, and performance
- Provides methods for data insertion and retrieval

### StockDataFetcher (fetch_data.py)
- Interfaces with Polygon.io API
- Fetches stock prices and company details
- Implements rate limiting for API calls

### IndexConstructor (index_construction.py)
- Constructs equal-weighted indices
- Tracks index performance
- Analyzes composition changes

### Dashboard (dashboard.py)
- Streamlit-based web interface
- Interactive data visualization
- Date range selection for analysis

## Data Sources

This project uses the following data sources:
- Market data from [Polygon.io](https://polygon.io/)
- Local SQLite database for data storage

## Database Schema

The project uses SQLite with the following main tables:
- `ticker_details`: Company information
- `stock_prices`: Daily price data
- `index_composition`: Index constituent weights
- `index_performance`: Daily index returns
- `index_composition_changes`: Tracking of constituent changes

## Dependencies

- `streamlit`: Web application framework
- `pandas`: Data manipulation and analysis
- `plotly`: Interactive visualizations
- `requests`: API communication
- `python-dotenv`: Environment variable management
- `sqlite3`: Database management

## Export Functionality

The export module provides functionality to:
- Export index performance data to CSV
- Export index composition data to CSV
- Customize export queries and filenames

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Data provided by Polygon.io
- Built with Streamlit and Plotly
- SQLite for efficient data storage

## Contact

Your Name - [@niharkhera] (https://github.com/niharkhera/)

Project Link: https://github.com/niharkhera/data_projects
