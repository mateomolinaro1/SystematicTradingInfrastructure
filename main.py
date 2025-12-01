import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import wrds
from ib_insync import *

universe = db.raw_sql("""
    SELECT a.permno, a.permco, a.ticker, a.shrcd, a.exchcd,
           a.namedt, a.nameendt,
           b.date, b.ret, b.prc, b.shrout, b.vol
    FROM crsp.msenames AS a
    JOIN crsp.msf AS b
      ON a.permno = b.permno
     AND b.date BETWEEN a.namedt AND a.nameendt
    WHERE a.exchcd IN (1, 2, 3)      -- NYSE, AMEX, NASDAQ
      AND a.shrcd IN (10, 11)        -- Common shares only
      AND b.date >= '01/01/2000'
    ORDER BY b.date, a.permno
""", date_cols=['namedt', 'nameendt', 'date'])
universe.to_csv(r'.\data\universe.csv', index=False)
sp500 = db.raw_sql("""
                        select a.*, b.date, b.ret
                        from crsp.msp500list as a,
                        crsp.msf as b
                        where a.permno=b.permno
                        and b.date >= a.start and b.date<= a.ending
                        and b.date>='01/01/2000'
                        order by date;
                        """, date_cols=['start', 'ending', 'date'])
mse = db.raw_sql("""
                        select comnam, ncusip, namedt, nameendt, 
                        permno, shrcd, exchcd, hsiccd, ticker
                        from crsp.msenames
                        """, date_cols=['namedt', 'nameendt'])
mse['nameendt']=mse['nameendt'].fillna(pd.to_datetime('today'))

ib = IB()
ib.connect(host='127.0.0.1', port=4002, clientId=1)

# Define a contract (example: Apple stock)
contract = Stock(symbol='MSFT', exchange='SMART', currency='USD')

# Request historical data
bars = dh.ib.reqHistoricalData(
    contract=contract,
    endDateTime='',
    durationStr='1 M',        # past 1 month
    barSizeSetting='1 day',   # daily bars
    whatToShow='ADJUSTED_LAST',    # or TRADES, BID, ASK
    useRTH=True,              # Regular Trading Hours only
    formatDate=1
)

# Convert to a pandas DataFrame
df2 = util.df(bars)

ib_prices = pd.DataFrame(data=np.nan,
                                 index=dh.dates,
                                 columns=dh.tickers_across_dates)

wu = pd.read_csv(r'.\data\wrds_universe.csv',
                 index_col='date',
                 parse_dates=['date'])
wu.head()

exchange_ib = dh.wrds_universe[dh.wrds_universe['ticker'] == 'JJSF']
exchange_ib = exchange_ib.sort_index(ascending=False).iloc[0]['exchange_ib']

ib_prices_dct={'AAPL':df,'MSFT':df2}
