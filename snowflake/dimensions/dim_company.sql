-- Dimension table for company information
CREATE OR REPLACE TABLE DIM_COMPANY (
    company_id STRING NOT NULL,
    symbol STRING NOT NULL,
    company_name STRING,
    sector STRING,
    industry STRING,
    exchange STRING,
    is_active BOOLEAN DEFAULT TRUE,
    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    valid_to TIMESTAMP_NTZ DEFAULT '9999-12-31 23:59:59.999',
    current_flag BOOLEAN DEFAULT TRUE,
    dwh_created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dwh_updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_company PRIMARY KEY (company_id, valid_from)
);

-- Create a stream for change data capture
CREATE OR REPLACE STREAM STREAM_DIM_COMPANY ON TABLE DIM_COMPANY;

-- Create a view to show current records
CREATE OR REPLACE VIEW VW_CURRENT_COMPANIES AS
SELECT * FROM DIM_COMPANY WHERE current_flag = TRUE;
