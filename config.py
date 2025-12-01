from pathlib import Path

ROOT = Path(__file__).resolve().parent

WRDS_USERNAME = 'mateo_molinaro'
IB_HOST = '127.0.0.1'
IB_PORT = 4002
IB_CLIENT_ID = 1

WRDS_REQUEST = """
WITH base AS (
    SELECT
        a.ticker, a.exchcd, a.permno, a.permco,
        a.namedt, a.nameendt,
        b.date, b.ret, b.prc, b.shrout, b.vol,
        ABS(b.prc) * b.shrout * 1000 AS market_cap
    FROM crsp.msenames AS a
    JOIN crsp.dsf AS b
      ON a.permno = b.permno
     AND b.date BETWEEN a.namedt AND a.nameendt
    WHERE a.exchcd IN (1, 2, 3)          -- NYSE, AMEX, NASDAQ
      AND a.shrcd IN (10, 11)            -- Common shares only
      AND b.date >= '01/01/2014'
      AND b.prc IS NOT NULL              -- ensure valid price
      AND b.vol IS NOT NULL              -- ensure valid volume
      AND b.prc != 0                     -- avoid zero-price issues
      AND ABS(b.prc) * b.vol >= 10000000 -- Dollar volume ≥ $10M
)
SELECT *
FROM (
    SELECT *,
           RANK() OVER (PARTITION BY date ORDER BY market_cap DESC) AS mcap_rank
    FROM base
) ranked
WHERE mcap_rank <= 1000
ORDER BY date, mcap_rank;
"""

FIELDS_WRDS_TO_KEEP_FOR_UNIVERSE = ['ticker',
                                    'exchcd',
                                    'permno',
                                    'permco',
                                    'namedt',
                                    'nameendt',
                                    'date']
DATE_COLS = [
    'namedt',
    'nameendt',
    'date'
]
SAVING_CONFIG_UNIVERSE = {
    'gross_query': {
        'path': '.\\data\\wrds_gross_query.csv',
        'extension': 'csv'
    },
    'universe': {
        'path': '.\\data\\wrds_universe.csv',
        'extension': 'csv'
    }
}
RETURN_BOOL_UNIVERSE = False
SAVING_CONFIG_PRICES = {
    'prices': {
        'path': '.\\data\\wrds_universe_prices.csv',
        'extension': 'csv'
    }
}
RETURN_BOOL_PRICES = False
RETURN_BOOL_RETURNS = False

# Fetching IB historical prices config
END_DATE = ''
PAST_PERIOD = '10 Y'
FREQUENCY = '1 day'
DATA_PRICES = 'ADJUSTED_LAST'
USE_RTH = True
FORMAT_DATE = 1
SAVE_PRICES = True
RETURN_BOOL_IB_PRICES = False

