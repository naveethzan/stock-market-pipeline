-- Fact table for daily stock prices
CREATE OR REPLACE TABLE FACT_STOCK_PRICES (
    stock_price_id STRING NOT NULL,
    company_id STRING NOT NULL,
    date_id DATE NOT NULL,
    open_price NUMBER(20, 6),
    high_price NUMBER(20, 6),
    low_price NUMBER(20, 6),
    close_price NUMBER(20, 6),
    volume NUMBER(38, 0),
    adjusted_close NUMBER(20, 6),
    daily_return NUMBER(10, 6),
    sma_5 NUMBER(20, 6),
    sma_20 NUMBER(20, 6),
    rsi_14 NUMBER(10, 6),
    batch_id STRING NOT NULL,
    dwh_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dwh_updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_fact_stock_prices PRIMARY KEY (stock_price_id),
    CONSTRAINT fk_company FOREIGN KEY (company_id) REFERENCES DIM_COMPANY(company_id),
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES DIM_DATE(date_id)
);

-- Create a stream for change data capture
CREATE OR REPLACE STREAM STREAM_FACT_STOCK_PRICES ON TABLE FACT_STOCK_PRICES;

-- Create a view for the latest stock prices
CREATE OR REPLACE VIEW VW_LATEST_STOCK_PRICES AS
SELECT 
    c.symbol,
    c.company_name,
    d.date_id as trade_date,
    f.open_price,
    f.high_price,
    f.low_price,
    f.close_price,
    f.volume,
    f.adjusted_close,
    f.daily_return,
    f.sma_5,
    f.sma_20,
    f.rsi_14,
    f.batch_id,
    f.dwh_updated_at as last_updated
FROM FACT_STOCK_PRICES f
JOIN DIM_COMPANY c ON f.company_id = c.company_id AND c.current_flag = TRUE
JOIN DIM_DATE d ON f.date_id = d.date_id;

-- Create a materialized view for performance
CREATE OR REPLACE MATERIALIZED VIEW MV_DAILY_STOCK_METRICS AS
SELECT 
    date_id,
    COUNT(DISTINCT company_id) as num_companies,
    SUM(volume) as total_volume,
    AVG(daily_return) as avg_daily_return,
    STDDEV(daily_return) as stddev_daily_return,
    SUM(CASE WHEN daily_return > 0 THEN 1 ELSE 0 END) as advancing_issues,
    SUM(CASE WHEN daily_return < 0 THEN 1 ELSE 0 END) as declining_issues,
    CURRENT_TIMESTAMP() as last_updated
FROM FACT_STOCK_PRICES
GROUP BY date_id;
