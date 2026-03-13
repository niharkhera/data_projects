-- data/init.sql

CREATE TABLE IF NOT EXISTS ticker_details (
    ticker VARCHAR PRIMARY KEY,
    active BOOLEAN,
    name VARCHAR,
    market VARCHAR,
    locale VARCHAR,
    primary_exchange VARCHAR,
    type VARCHAR,
    currency_name VARCHAR,
    cik VARCHAR,
    description TEXT,
    homepage_url VARCHAR,
    list_date DATE,
    market_cap DOUBLE PRECISION,
    phone_number VARCHAR,
    total_employees INTEGER,
    address1 VARCHAR,
    address2 VARCHAR,
    city VARCHAR,
    state VARCHAR,
    postal_code VARCHAR,
    branding_icon_url VARCHAR,
    branding_logo_url VARCHAR,
    sic_code VARCHAR,
    sic_description VARCHAR,
    ticker_root VARCHAR,
    ticker_suffix VARCHAR,
    weighted_shares_outstanding DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS stock_prices (
    ticker VARCHAR,
    timestamp BIGINT,
    date DATE,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    transactions INTEGER,
    volume_weighted_avg DOUBLE PRECISION,
    is_otc BOOLEAN,
    is_adjusted BOOLEAN,
    PRIMARY KEY (ticker, timestamp)
);

CREATE TABLE IF NOT EXISTS index_composition (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    ticker VARCHAR NOT NULL,
    close_price DOUBLE PRECISION NOT NULL,
    weight DOUBLE PRECISION NOT NULL,
    market_cap DOUBLE PRECISION,
    index_type VARCHAR NOT NULL,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (date, ticker, index_type, update_time)
);

CREATE TABLE IF NOT EXISTS index_performance (
    date DATE NOT NULL,
    index_price DOUBLE PRECISION NOT NULL,
    daily_return DOUBLE PRECISION NOT NULL,
    index_type VARCHAR NOT NULL,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, index_type, update_time)
);

CREATE TABLE IF NOT EXISTS index_composition_changes (
    date DATE NOT NULL,
    symbols TEXT NOT NULL,
    prev_date DATE,
    prev_symbols TEXT,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, update_time)
);