CREATE TABLE IF NOT EXISTS candle_weekly (
    asset_id BIGINT,
    quote_asset_id BIGINT,
    time BIGINT,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    PRIMARY KEY (asset_id, quote_asset_id, time)
);

CREATE TABLE IF NOT EXISTS candle_daily (
    asset_id BIGINT,
    quote_asset_id BIGINT,
    time BIGINT,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    PRIMARY KEY (asset_id, quote_asset_id, time)
);

CREATE TABLE IF NOT EXISTS candle_hourly (
    asset_id BIGINT,
    quote_asset_id BIGINT,
    time BIGINT,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    PRIMARY KEY (asset_id, quote_asset_id, time)
);

CREATE TABLE IF NOT EXISTS candle_fifteen (
    asset_id BIGINT,
    quote_asset_id BIGINT,
    time BIGINT,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    PRIMARY KEY (asset_id, quote_asset_id, time)
);