CREATE OR REPLACE PROCEDURE refresh_candle_daily_incremental(specific_prices_json JSONB DEFAULT NULL)
LANGUAGE plpgsql
AS $$
DECLARE
    v_last_time BIGINT;
    v_new_time BIGINT;
    v_start_time BIGINT;
    v_has_specific_prices BOOLEAN := specific_prices_json IS NOT NULL AND specific_prices_json != '[]'::JSONB;
BEGIN
    -- Set a statement timeout (10 minutes)
    SET LOCAL statement_timeout = '10min';

    -- Log start
    RAISE NOTICE 'Starting refresh_candle_daily_incremental at % (has_specific_prices: %)', CURRENT_TIMESTAMP, v_has_specific_prices;

    -- Get the last processed time from the tracking table
    SELECT vrl.last_time INTO v_last_time
    FROM view_refresh_log vrl
    WHERE vrl.view_name = 'candle_daily';

    RAISE NOTICE 'v_last_time: %', v_last_time;

    -- Get the latest time from the price table
    SELECT MAX(time) INTO v_new_time
    FROM price
    WHERE outlier IS NULL;

    RAISE NOTICE 'v_new_time: %', v_new_time;

    -- Calculate the start of the candle period (0 for initial load)
    SELECT COALESCE(FLOOR(v_last_time / 86400) * 86400, 0) INTO v_start_time;

    RAISE NOTICE 'v_start_time: %', v_start_time;

    -- Log load type
    IF v_last_time IS NULL OR v_start_time = 0 THEN
        RAISE NOTICE 'Performing full initial load';
    ELSE
        RAISE NOTICE 'Performing incremental load';
    END IF;

    -- If no new data, exit early
    IF v_new_time IS NULL OR v_new_time <= v_start_time THEN
        RAISE NOTICE 'No new data, exiting';
        RETURN;
    END IF;

    -- Log before main query
    RAISE NOTICE 'Starting main query at %', CURRENT_TIMESTAMP;

    -- Upsert new/updated daily candles
    WITH daily_prices AS (
        SELECT
            p.asset_id,
            p.quote_asset_id,
            FLOOR(p.time / 86400) * 86400 AS time,
            MIN(p.price) AS low_price,
            MAX(p.price) AS high_price,
            SUM(p.amount1) AS volume,
            (SELECT p2.price
             FROM price p2
             WHERE p2.asset_id = p.asset_id
               AND p2.quote_asset_id = p.quote_asset_id
               AND p2.time = MAX(p.time)
               AND p2.outlier IS NULL
             ORDER BY p2.time DESC, p2.tx_id DESC, p2.tx_swap_idx DESC
             LIMIT 1
            ) AS close_price
        FROM price p
        WHERE p.outlier IS NULL
          AND p.time >= v_start_time
          AND p.time <= v_new_time
          AND (v_has_specific_prices = FALSE OR (p.asset_id, p.quote_asset_id) IN (
              SELECT DISTINCT
                  (elem->>'asset_id')::BIGINT,
                  (elem->>'quote_asset_id')::BIGINT
              FROM jsonb_array_elements(specific_prices_json) AS elem
          ))
        GROUP BY p.asset_id, p.quote_asset_id, FLOOR(p.time / 86400) * 86400
    ),
    prior_candles AS (
        SELECT
            cd.asset_id,
            cd.quote_asset_id,
            cd.close AS prior_close
        FROM candle_daily cd
        WHERE cd.time = (
            SELECT MAX(time)
            FROM candle_daily cd2
            WHERE cd2.asset_id = cd.asset_id
              AND cd2.quote_asset_id = cd.quote_asset_id
              AND cd2.time < v_start_time
        )
    ),
    daily_candles AS (
        SELECT
            dp.time,
            dp.asset_id,
            dp.quote_asset_id,
            dp.low_price,
            dp.high_price,
            dp.volume,
            dp.close_price,
            COALESCE(
                LAG(dp.close_price) OVER (
                    PARTITION BY dp.asset_id, dp.quote_asset_id
                    ORDER BY dp.time
                ),
                pc.prior_close,
                dp.close_price
            ) AS open_price
        FROM daily_prices dp
        LEFT JOIN prior_candles pc
            ON pc.asset_id = dp.asset_id
            AND pc.quote_asset_id = dp.quote_asset_id
    )
    INSERT INTO candle_daily (
        asset_id,
        quote_asset_id,
        time,
        open,
        high,
        low,
        close,
        volume
    )
    SELECT
        asset_id,
        quote_asset_id,
        time,
        open_price AS open,
        high_price AS high,
        low_price AS low,
        close_price AS close,
        volume
    FROM daily_candles
    WHERE close_price IS NOT NULL
    ON CONFLICT (asset_id, quote_asset_id, time)
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume;

    -- Log after insert
    RAISE NOTICE 'Completed insert at %', CURRENT_TIMESTAMP;

    -- Update the tracking table
    INSERT INTO view_refresh_log (view_name, last_refresh, last_time)
    VALUES ('candle_daily', CURRENT_TIMESTAMP, v_new_time)
    ON CONFLICT (view_name)
    DO UPDATE SET
        last_refresh = CURRENT_TIMESTAMP,
        last_time = v_new_time;

    RAISE NOTICE 'Completed refresh_candle_daily_incremental at %', CURRENT_TIMESTAMP;

    COMMIT;
END;
$$;


-- 15 MIN
CREATE OR REPLACE PROCEDURE refresh_candle_fifteen_incremental(specific_prices_json JSONB DEFAULT NULL)
LANGUAGE plpgsql
AS $$
DECLARE
    v_last_time BIGINT;
    v_new_time BIGINT;
    v_start_time BIGINT;
    v_has_specific_prices BOOLEAN := specific_prices_json IS NOT NULL AND specific_prices_json != '[]'::JSONB;
BEGIN
    -- Set a statement timeout (10 minutes)
    SET LOCAL statement_timeout = '10min';

    -- Log start
    RAISE NOTICE 'Starting refresh_candle_fifteen_incremental at % (has_specific_prices: %)', CURRENT_TIMESTAMP, v_has_specific_prices;

    -- Get the last processed time from the tracking table
    SELECT vrl.last_time INTO v_last_time
    FROM view_refresh_log vrl
    WHERE vrl.view_name = 'candle_fifteen';

    RAISE NOTICE 'v_last_time: %', v_last_time;

    -- Get the latest time from the price table
    SELECT MAX(time) INTO v_new_time
    FROM price
    WHERE outlier IS NULL;

    RAISE NOTICE 'v_new_time: %', v_new_time;

    -- Calculate the start of the candle period (0 for initial load)
    SELECT COALESCE(FLOOR(v_last_time / 900) * 900, 0) INTO v_start_time;

    RAISE NOTICE 'v_start_time: %', v_start_time;

    -- Log load type
    IF v_last_time IS NULL OR v_start_time = 0 THEN
        RAISE NOTICE 'Performing full initial load';
    ELSE
        RAISE NOTICE 'Performing incremental load';
    END IF;

    -- If no new data, exit early
    IF v_new_time IS NULL OR v_new_time <= v_start_time THEN
        RAISE NOTICE 'No new data, exiting';
        RETURN;
    END IF;

    -- Log before main query
    RAISE NOTICE 'Starting main query at %', CURRENT_TIMESTAMP;

    -- Upsert new/updated 15-minute candles
    WITH fifteen_prices AS (
        SELECT
            p.asset_id,
            p.quote_asset_id,
            FLOOR(p.time / 900) * 900 AS time,
            MIN(p.price) AS low_price,
            MAX(p.price) AS high_price,
            SUM(p.amount1) AS volume,
            (SELECT p2.price
             FROM price p2
             WHERE p2.asset_id = p.asset_id
               AND p2.quote_asset_id = p.quote_asset_id
               AND p2.time = MAX(p.time)
               AND p2.outlier IS NULL
             ORDER BY p2.time DESC, p2.tx_id DESC, p2.tx_swap_idx DESC
             LIMIT 1
            ) AS close_price
        FROM price p
        WHERE p.outlier IS NULL
          AND p.time >= v_start_time
          AND p.time <= v_new_time
          AND (v_has_specific_prices = FALSE OR (p.asset_id, p.quote_asset_id) IN (
              SELECT DISTINCT
                  (elem->>'asset_id')::BIGINT,
                  (elem->>'quote_asset_id')::BIGINT
              FROM jsonb_array_elements(specific_prices_json) AS elem
          ))
        GROUP BY p.asset_id, p.quote_asset_id, FLOOR(p.time / 900) * 900
    ),
    prior_candles AS (
        SELECT
            cf.asset_id,
            cf.quote_asset_id,
            cf.close AS prior_close
        FROM candle_fifteen cf
        WHERE cf.time = (
            SELECT MAX(time)
            FROM candle_fifteen cf2
            WHERE cf2.asset_id = cf.asset_id
              AND cf2.quote_asset_id = cf.quote_asset_id
              AND cf2.time < v_start_time
        )
    ),
    fifteen_candles AS (
        SELECT
            fp.time,
            fp.asset_id,
            fp.quote_asset_id,
            fp.low_price,
            fp.high_price,
            fp.volume,
            fp.close_price,
            COALESCE(
                LAG(fp.close_price) OVER (
                    PARTITION BY fp.asset_id, fp.quote_asset_id
                    ORDER BY fp.time
                ), -- Prefer using LAG, very efficient for initial loading
                pc.prior_close, -- Fall back to using prior known candle, necessary for incremental updates
                fp.close_price
            ) AS open_price
        FROM fifteen_prices fp
        LEFT JOIN prior_candles pc
            ON pc.asset_id = fp.asset_id
            AND pc.quote_asset_id = fp.quote_asset_id
    )
    INSERT INTO candle_fifteen (
        asset_id,
        quote_asset_id,
        time,
        open,
        high,
        low,
        close,
        volume
    )
    SELECT
        asset_id,
        quote_asset_id,
        time,
        open_price AS open,
        high_price AS high,
        low_price AS low,
        close_price AS close,
        volume
    FROM fifteen_candles
    WHERE close_price IS NOT NULL
    ON CONFLICT (asset_id, quote_asset_id, time)
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume;

    -- Log after insert
    RAISE NOTICE 'Completed insert at %', CURRENT_TIMESTAMP;

    -- Update the tracking table
    INSERT INTO view_refresh_log (view_name, last_refresh, last_time)
    VALUES ('candle_fifteen', CURRENT_TIMESTAMP, v_new_time)
    ON CONFLICT (view_name)
    DO UPDATE SET
        last_refresh = CURRENT_TIMESTAMP,
        last_time = v_new_time;

    RAISE NOTICE 'Completed refresh_candle_fifteen_incremental at %', CURRENT_TIMESTAMP;

    COMMIT;
END;
$$;


-- Hourly

CREATE OR REPLACE PROCEDURE refresh_candle_hourly_incremental(specific_prices_json JSONB DEFAULT NULL)
LANGUAGE plpgsql
AS $$
DECLARE
    v_last_time BIGINT;
    v_new_time BIGINT;
    v_start_time BIGINT;
    v_has_specific_prices BOOLEAN := specific_prices_json IS NOT NULL AND specific_prices_json != '[]'::JSONB;
BEGIN
    -- Set a statement timeout (10 minutes)
    SET LOCAL statement_timeout = '10min';

    -- Log start
    RAISE NOTICE 'Starting refresh_candle_hourly_incremental at % (has_specific_prices: %)', CURRENT_TIMESTAMP, v_has_specific_prices;

    -- Get the last processed time from the tracking table
    SELECT vrl.last_time INTO v_last_time
    FROM view_refresh_log vrl
    WHERE vrl.view_name = 'candle_hourly';

    RAISE NOTICE 'v_last_time: %', v_last_time;

    -- Get the latest time from the price table
    SELECT MAX(time) INTO v_new_time
    FROM price
    WHERE outlier IS NULL;

    RAISE NOTICE 'v_new_time: %', v_new_time;

    -- Calculate the start of the candle period (0 for initial load)
    SELECT COALESCE(FLOOR(v_last_time / 3600) * 3600, 0) INTO v_start_time;

    RAISE NOTICE 'v_start_time: %', v_start_time;

    -- Log load type
    IF v_last_time IS NULL OR v_start_time = 0 THEN
        RAISE NOTICE 'Performing full initial load';
    ELSE
        RAISE NOTICE 'Performing incremental load';
    END IF;

    -- If no new data, exit early
    IF v_new_time IS NULL OR v_new_time <= v_start_time THEN
        RAISE NOTICE 'No new data, exiting';
        RETURN;
    END IF;

    -- Log before main query
    RAISE NOTICE 'Starting main query at %', CURRENT_TIMESTAMP;

    -- Upsert new/updated hourly candles
    WITH hourly_prices AS (
        SELECT
            p.asset_id,
            p.quote_asset_id,
            FLOOR(p.time / 3600) * 3600 AS time,
            MIN(p.price) AS low_price,
            MAX(p.price) AS high_price,
            SUM(p.amount1) AS volume,
            (SELECT p2.price
             FROM price p2
             WHERE p2.asset_id = p.asset_id
               AND p2.quote_asset_id = p.quote_asset_id
               AND p2.time = MAX(p.time)
               AND p2.outlier IS NULL
             ORDER BY p2.time DESC, p2.tx_id DESC, p2.tx_swap_idx DESC
             LIMIT 1
            ) AS close_price
        FROM price p
        WHERE p.outlier IS NULL
          AND p.time >= v_start_time
          AND p.time <= v_new_time
          AND (v_has_specific_prices = FALSE OR (p.asset_id, p.quote_asset_id) IN (
              SELECT DISTINCT
                  (elem->>'asset_id')::BIGINT,
                  (elem->>'quote_asset_id')::BIGINT
              FROM jsonb_array_elements(specific_prices_json) AS elem
          ))
        GROUP BY p.asset_id, p.quote_asset_id, FLOOR(p.time / 3600) * 3600
    ),
    prior_candles AS (
        SELECT
            ch.asset_id,
            ch.quote_asset_id,
            ch.close AS prior_close
        FROM candle_hourly ch
        WHERE ch.time = (
            SELECT MAX(time)
            FROM candle_hourly ch2
            WHERE ch2.asset_id = ch.asset_id
              AND ch2.quote_asset_id = ch.quote_asset_id
              AND ch2.time < v_start_time
        )
    ),
    hourly_candles AS (
        SELECT
            hp.time,
            hp.asset_id,
            hp.quote_asset_id,
            hp.low_price,
            hp.high_price,
            hp.volume,
            hp.close_price,
            COALESCE(
                LAG(hp.close_price) OVER (
                    PARTITION BY hp.asset_id, hp.quote_asset_id
                    ORDER BY hp.time
                ),
                pc.prior_close,
                hp.close_price
            ) AS open_price
        FROM hourly_prices hp
        LEFT JOIN prior_candles pc
            ON pc.asset_id = hp.asset_id
            AND pc.quote_asset_id = hp.quote_asset_id
    )
    INSERT INTO candle_hourly (
        asset_id,
        quote_asset_id,
        time,
        open,
        high,
        low,
        close,
        volume
    )
    SELECT
        asset_id,
        quote_asset_id,
        time,
        open_price AS open,
        high_price AS high,
        low_price AS low,
        close_price AS close,
        volume
    FROM hourly_candles
    WHERE close_price IS NOT NULL
    ON CONFLICT (asset_id, quote_asset_id, time)
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume;

    -- Log after insert
    RAISE NOTICE 'Completed insert at %', CURRENT_TIMESTAMP;

    -- Update the tracking table
    INSERT INTO view_refresh_log (view_name, last_refresh, last_time)
    VALUES ('candle_hourly', CURRENT_TIMESTAMP, v_new_time)
    ON CONFLICT (view_name)
    DO UPDATE SET
        last_refresh = CURRENT_TIMESTAMP,
        last_time = v_new_time;

    RAISE NOTICE 'Completed refresh_candle_hourly_incremental at %', CURRENT_TIMESTAMP;

    COMMIT;
END;
$$;


--- Weekly

 CREATE OR REPLACE PROCEDURE refresh_candle_weekly_incremental(specific_prices_json JSONB DEFAULT NULL)
 LANGUAGE plpgsql
 AS $$
 DECLARE                                                                                                                               
     v_last_time BIGINT;                                                                                                               
     v_new_time BIGINT;                                                                                                                
     v_start_time BIGINT;                                                                                                              
     v_has_specific_prices BOOLEAN := specific_prices_json IS NOT NULL AND specific_prices_json != '[]'::JSONB;                        
 BEGIN                                                                                                                                 
     -- Set a statement timeout (10 minutes)                                                                                           
     SET LOCAL statement_timeout = '10min';                                                                                            
                                                                                                                                       
     -- Log start                                                                                                                      
     RAISE NOTICE 'Starting refresh_candle_weekly_incremental at % (has_specific_prices: %)', CURRENT_TIMESTAMP, v_has_specific_prices;
                                                                                                                                       
     -- Get the last processed time from the tracking table                                                                            
     SELECT vrl.last_time INTO v_last_time                                                                                             
     FROM view_refresh_log vrl                                                                                                         
     WHERE vrl.view_name = 'candle_weekly';                                                                                            
                                                                                                                                       
     RAISE NOTICE 'v_last_time: %', v_last_time;                                                                                       
                                                                                                                                       
     -- Get the latest time from the price table                                                                                       
     SELECT MAX(time) INTO v_new_time                                                                                                  
     FROM price                                                                                                                        
     WHERE outlier IS NULL;                                                                                                            
                                                                                                                                       
     RAISE NOTICE 'v_new_time: %', v_new_time;                                                                                         
                                                                                                                                       
     -- Calculate the start of the candle period containing v_last_time (0 for initial load)                                           
     SELECT COALESCE(FLOOR((v_last_time - 345600) / 604800) * 604800 + 345600, 0) INTO v_start_time;
                                                                                                                                       
     RAISE NOTICE 'v_start_time: %', v_start_time;                                                                                     
                                                                                                                                       
     -- Log load type                                                                                                                  
     IF v_last_time IS NULL OR v_start_time = 0 THEN                                                                                   
         RAISE NOTICE 'Performing full initial load';                                                                                  
     ELSE                                                                                                                              
         RAISE NOTICE 'Performing incremental load';                                                                                   
     END IF;                                                                                                                           
                                                                                                                                       
     -- If no new data, exit early                                                                                                     
     IF v_new_time IS NULL OR v_new_time <= v_start_time THEN                                                                          
         RAISE NOTICE 'No new data, exiting';                                                                                          
         RETURN;                                                                                                                       
     END IF;                                                                                                                           
                                                                                                                                       
     -- Log before main query                                                                                                          
     RAISE NOTICE 'Starting main query at %', CURRENT_TIMESTAMP;                                                                       
                                                                                                                                       
     -- Upsert new/updated weekly candles                                                                                              
     WITH weekly_prices AS (                                                                                                           
         SELECT                                                                                                                        
             p.asset_id,                                                                                                               
             p.quote_asset_id,                                                                                                         
             FLOOR((p.time - 345600) / 604800) * 604800 + 345600 AS time,
             MIN(p.price) AS low_price,                                                                                                
             MAX(p.price) AS high_price,                                                                                               
             SUM(p.amount1) AS volume,                                                                                                 
             (SELECT p2.price                                                                                                          
              FROM price p2                                                                                                            
              WHERE p2.asset_id = p.asset_id                                                                                           
                AND p2.quote_asset_id = p.quote_asset_id                                                                               
                AND p2.time = MAX(p.time)                                                                                              
                AND p2.outlier IS NULL                                                                                                 
              ORDER BY p2.time DESC, p2.tx_id DESC, p2.tx_swap_idx DESC                                                                
              LIMIT 1                                                                                                                  
             ) AS close_price                                                                                                          
         FROM price p                                                                                                                  
         WHERE p.outlier IS NULL                                                                                                       
           AND p.time >= v_start_time                                                                                                  
           AND p.time <= v_new_time                                                                                                    
           AND (v_has_specific_prices = FALSE OR (p.asset_id, p.quote_asset_id) IN (                                                   
               SELECT DISTINCT                                                                                                         
                   (elem->>'asset_id')::BIGINT,                                                                                        
                   (elem->>'quote_asset_id')::BIGINT                                                                                   
               FROM jsonb_array_elements(specific_prices_json) AS elem                                                                 
           ))                                                                                                                          
         GROUP BY p.asset_id, p.quote_asset_id, FLOOR((p.time - 345600) / 604800) * 604800 + 345600
     ),                                                                                                                                
     prior_candles AS (                                                                                                                
         SELECT                                                                                                                        
             cw.asset_id,                                                                                                              
             cw.quote_asset_id,                                                                                                        
             cw.close AS prior_close                                                                                                   
         FROM candle_weekly cw                                                                                                         
         WHERE cw.time = (                                                                                                             
             SELECT MAX(time)                                                                                                          
             FROM candle_weekly cw2                                                                                                    
             WHERE cw2.asset_id = cw.asset_id                                                                                          
               AND cw2.quote_asset_id = cw.quote_asset_id                                                                              
               AND cw2.time < v_start_time                                                                                             
         )                                                                                                                             
     ),                                                                                                                                
     weekly_candles AS (                                                                                                               
         SELECT                                                                                                                        
             wp.time,                                                                                                                  
             wp.asset_id,                                                                                                              
             wp.quote_asset_id,                                                                                                        
             wp.low_price,                                                                                                             
             wp.high_price,                                                                                                            
             wp.volume,                                                                                                                
             wp.close_price,                                                                                                           
             COALESCE(                                                                                                                 
                 LAG(wp.close_price) OVER (                                                                                            
                     PARTITION BY wp.asset_id, wp.quote_asset_id                                                                       
                     ORDER BY wp.time                                                                                                  
                 ),                                                                                                                    
                 pc.prior_close,                                                                                                       
                 wp.close_price                                                                                                        
             ) AS open_price                                                                                                           
         FROM weekly_prices wp                                                                                                         
         LEFT JOIN prior_candles pc                                                                                                    
             ON pc.asset_id = wp.asset_id                                                                                              
             AND pc.quote_asset_id = wp.quote_asset_id                                                                                 
     )                                                                                                                                 
     INSERT INTO candle_weekly (                                                                                                       
         asset_id,                                                                                                                     
         quote_asset_id,                                                                                                               
         time,                                                                                                                         
         open,                                                                                                                         
         high,                                                                                                                         
         low,                                                                                                                          
         close,                                                                                                                        
         volume                                                                                                                        
     )                                                                                                                                 
     SELECT                                                                                                                            
         asset_id,                                                                                                                     
         quote_asset_id,                                                                                                               
         time,                                                                                                                         
         open_price AS open,                                                                                                           
         high_price AS high,                                                                                                           
         low_price AS low,                                                                                                             
         close_price AS close,                                                                                                         
         volume                                                                                                                        
     FROM weekly_candles                                                                                                               
     WHERE close_price IS NOT NULL                                                                                                     
     ON CONFLICT (asset_id, quote_asset_id, time)                                                                                      
     DO UPDATE SET                                                                                                                     
         open = EXCLUDED.open,                                                                                                         
         high = EXCLUDED.high,                                                                                                         
         low = EXCLUDED.low,                                                                                                           
         close = EXCLUDED.close,                                                                                                       
         volume = EXCLUDED.volume;                                                                                                     
                                                                                                                                       
     -- Log after insert                                                                                                               
     RAISE NOTICE 'Completed insert at %', CURRENT_TIMESTAMP;                                                                          
                                                                                                                                       
     -- Update the tracking table                                                                                                      
     INSERT INTO view_refresh_log (view_name, last_refresh, last_time)                                                                 
     VALUES ('candle_weekly', CURRENT_TIMESTAMP, v_new_time)                                                                           
     ON CONFLICT (view_name)                                                                                                           
     DO UPDATE SET                                                                                                                     
         last_refresh = CURRENT_TIMESTAMP,                                                                                             
         last_time = v_new_time;                                                                                                       
                                                                                                                                       
     RAISE NOTICE 'Completed refresh_candle_weekly_incremental at %', CURRENT_TIMESTAMP;                                               
                                                                                                                                       
     COMMIT;                                                                                                                           
 END;                                                                                                                                  
$$;