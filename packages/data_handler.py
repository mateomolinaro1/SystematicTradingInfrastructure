import pandas as pd
import numpy as np
import pickle
import os
import time
from typing import List, Union, Tuple
import logging
import wrds
from ib_insync import *
import asyncio
from tqdm.asyncio import tqdm_asyncio

logger = logging.getLogger(__name__)

class DataHandler:
    def __init__(self,
                 wrds_username:str='mateo_molinaro',
                 ib_host:str='127.0.0.1',
                 ib_port:int=4002,
                 ib_client_id:int=1)->None:
        self.wrds_username = wrds_username
        self.ib_host = ib_host
        self.ib_port = ib_port
        self.ib_client_id = ib_client_id

        self.wrds_db = None
        self.ib = None

        self.wrds_gross_query = None
        self.wrds_universe = None
        self.universe_prices_wrds = None
        self.universe_returns_wrds = None
        self.crsp_to_ib_exchange = {
            1: "NYSE",
            2: "AMEX",
            3: "NASDAQ",
            4: "ARCA",
            11: "BATS",
            31: "NASDAQ",
            32: "NASDAQ",
            33: "NASDAQ"
        }
        self.crps_exchcd_to_currency = {
            1:'USD',
            2:'USD',
            3:'USD'
        }
        self.tickers_across_dates = None
        self.dates = None

        self.universe_prices_ib = None
        self.universe_returns_ib = None

    def connect_wrds(self):
        """Establishes a connection to the WRDS database using the provided username."""
        self.wrds_db = wrds.Connection(wrds_username=self.wrds_username)

    def logout_wrds(self):
        """Logs out from the WRDS database connection."""
        if self.wrds_db is not None:
            self.wrds_db.close()
            self.wrds_db = None

    def connect_ib(self):
        """Establishes a connection to the Interactive Brokers API using the provided host, port, and client ID."""
        self.ib = IB()
        self.ib.connect(host=self.ib_host,
                        port=self.ib_port,
                        clientId=self.ib_client_id)
    # async def connect_ib(self):
    #     self.ib = IB()
    #     await self.ib.connectAsync(
    #         host=self.ib_host,
    #         port=self.ib_port,
    #         clientId=self.ib_client_id
    #     )

    def logout_ib(self):
        """Disconnects from the Interactive Brokers API."""
        if self.ib is not None:
            self.ib.disconnect()
            self.ib = None

    def fetch_wrds_historical_universe(self,
                                       wrds_request:str,
                                       fields_wrds_to_keep_for_universe:List[str],
                                       date_cols:List[str],
                                       saving_config:dict,
                                       return_bool:bool=False,
                                       )->Union[None,pd.DataFrame]:
        """Fetches historical universe from WRDS based on the provided SQL request."""
        # Ensure connection to WRDS
        if self.wrds_db is None:
            self.connect_wrds()

        # Query WRDS database
        self.wrds_gross_query = self.wrds_db.raw_sql(sql=wrds_request,
                                                     date_cols=date_cols)

        # As unique identifiers of WRDS/CRSP are PERMNO and not ticker but for IB it is ticker,
        # We'll drop duplicates rows based on (date, ticker) and keep the last occurrence
        self.wrds_gross_query = self.wrds_gross_query.drop_duplicates(subset=['date', 'ticker'],
                                                                      keep='last')

        self.tickers_across_dates = list(self.wrds_gross_query['ticker'].unique())
        with open(r'.\data\tickers_across_dates.pkl', 'wb') as f:
            pickle.dump(self.tickers_across_dates, f)
        self.dates = list(self.wrds_gross_query['date'].unique())
        with open(r'.\data\dates.pkl', 'wb') as f:
            pickle.dump(self.dates, f)

        # Save gross query if specified
        if 'gross_query' in saving_config:
            if saving_config['gross_query']['extension'] == 'csv':
                self.wrds_gross_query.to_csv(saving_config['gross_query']['path'],
                                            index=False)
            else:
                raise ValueError("Unsupported file extension for gross query. Use 'csv'.")


        universe = self.wrds_gross_query.copy()
        universe.index = universe['date']
        self.wrds_universe = universe

        # Format the universe DataFrame: mapping wrds data to ib data
        self.format_wrds_historical_universe(fields_wrds_to_keep_for_universe=fields_wrds_to_keep_for_universe)

        # Save to file if a saving path is provided
        if 'universe' in saving_config:
            if saving_config['universe']['extension'] == 'csv':
                self.wrds_universe.to_csv(saving_config['universe']['path'],
                                          index=True)
            else:
                raise ValueError("Unsupported file extension for universe. Use 'csv'.")

        if return_bool:
            return universe

    def format_wrds_historical_universe(self,
                                        fields_wrds_to_keep_for_universe:List[str])->None:
        """Formats the WRDS historical universe DataFrame."""
        if self.wrds_universe is None:
            raise ValueError("WRDS universe data is not loaded. Please fetch it first.")

        wrds_universe = self.wrds_universe.copy()
        self.wrds_universe = wrds_universe[fields_wrds_to_keep_for_universe].copy()
        self.wrds_universe['exchange'] = (
            self.wrds_universe['exchcd']
            .map(self.crsp_to_ib_exchange)
            .fillna("UNKNOWN")
        )
        self.wrds_universe['currency'] = (
            self.wrds_universe['exchcd']
            .map(self.crps_exchcd_to_currency)
            .fillna("UNKNOWN")
        )
        self.wrds_universe['exchange_ib'] = "SMART"  # Default to SMART for IB

    def get_wrds_historical_prices(self,
                                   saving_config:dict,
                                   return_bool:bool=False) -> Union[None, pd.DataFrame]:
        """Format self.wrds_gross_query to have a nice prices df."""
        if self.wrds_gross_query is None:
            raise ValueError("WRDS universe data is not loaded. Please fetch it first.")

        prices = self.wrds_gross_query.pivot(values='prc',
                                             index='date',
                                             columns='permno')
        self.universe_prices_wrds = prices

        if 'prices' in saving_config:
            if saving_config['prices']['extension'] == 'csv':
                prices.to_csv(saving_config['prices']['path'],
                              index=True)
            else:
                raise ValueError("Unsupported file extension for prices. Use 'csv'.")

        if return_bool:
            return prices

    def get_wrds_returns(self,
                         return_bool:bool=False) -> Union[None, pd.DataFrame]:
        """ Compute returns DataFrame from universe prices."""
        if self.universe_prices_wrds is None:
            raise ValueError("Universe prices data is not loaded. Please fetch it first.")
        returns = self.universe_prices_wrds.pct_change(fill_method=None)
        self.universe_returns_wrds = returns
        if return_bool:
            return returns

    def fetch_ib_historical_prices(self,
                                   end_date:str='',
                                   past_period:str='1 M',
                                   frequency:str='1 day',
                                   data_prices:str='ADJUSTED_LAST',
                                   use_rth:bool=True,
                                   format_date:int=1,
                                   save_prices:bool=True,
                                   return_bool:bool=False)->Union[None, pd.DataFrame]:
        """ Fetch historical prices from IB for all tickers in 'tickers_across_dates'"""
        logger.info("Starting IB historical fetch...")
        # --------------------------------------------------------
        # 0. Ensure IB connection
        # --------------------------------------------------------
        if self.ib is None:
            logger.info("IB connection not found — attempting to reconnect.")
            self.connect_ib()
            logger.info("Connected to IB.")

        # --------------------------------------------------------
        # 1. Load WRDS universe if needed
        # --------------------------------------------------------
        if self.wrds_universe is None:
            file_path = r".\data\wrds_universe.csv"
            if os.path.exists(file_path):
                logger.info(f"Loading WRDS universe from {file_path}")
                self.wrds_universe = pd.read_csv(file_path,
                                                 index_col='date',
                                                 parse_dates=['date'])
            else:
                logger.error("WRDS universe file not found.")
                raise ValueError("WRDS universe data is not loaded. Please fetch it first.")

        # --------------------------------------------------------
        # 2. Load tickers_across_dates if needed
        # --------------------------------------------------------
        if self.tickers_across_dates is None:
            file_path = r".\data\tickers_across_dates.pkl"
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                logger.info(f"Loading tickers from {file_path}")
                try:
                    with open(file_path, 'rb') as f:
                        self.tickers_across_dates = pickle.load(f)
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {e}")
                    raise ValueError("Failed to load tickers_across_dates from pickle file.")
            else:
                logger.error("Tickers file missing or empty.")
                raise ValueError("Tickers across dates data is not loaded. Please fetch WRDS universe first.")

        # --------------------------------------------------------
        # 3. Prepare dict for results
        # --------------------------------------------------------
        ib_prices_dct = {}
        wrds_df = self.wrds_universe.sort_index(ascending=False)

        # --------------------------------------------------------
        # 4. Helper: safe IB request with pacing retry
        # --------------------------------------------------------
        def safe_req(contract_ib):
            while True:
                try:
                    return self.ib.reqHistoricalData(
                        contract=contract_ib,
                        endDateTime=end_date,
                        durationStr=past_period,
                        barSizeSetting=frequency,
                        whatToShow=data_prices,
                        useRTH=use_rth,
                        formatDate=format_date
                    )
                except Exception as e:
                    msg = str(e).lower()
                    if "pacing" in msg or "rate" in msg or "violation" in msg:
                        print("⚠️ Pacing violation → waiting 30 seconds before retrying...")
                        logger.warning(f"Pacing violation for {contract_ib.symbol}, retrying...")
                        time.sleep(30)
                    else:
                        raise

        # --------------------------------------------------------
        # 4. Loop over tickers
        # --------------------------------------------------------
        for i,ticker in enumerate(self.tickers_across_dates):
            logger.info(f"Fetching ticker: {ticker} ({i+1}/{len(self.tickers_across_dates)})")
            print(f"Fetching ticker: {ticker} ({i+1}/{len(self.tickers_across_dates)})")

            # Extract the most recent data
            subset = wrds_df[wrds_df["ticker"] == ticker]
            if subset.empty:
                print(f"⚠️ WARNING: Ticker {ticker} not found in WRDS universe. Skipping.")
                continue
            exchange_ib = subset.iloc[0]["exchange_ib"]
            currency = subset.iloc[0]["currency"]
            primary_exchange = subset.iloc[0]["exchange"]

            # --------------------------------------------------------
            # IB Contract
            # --------------------------------------------------------
            contract = Stock(symbol=ticker,
                             exchange=exchange_ib,
                             primaryExchange=primary_exchange,
                             currency=currency
                             )

            # --------------------------------------------------------
            # Validate contract — avoid "No security definition" errors
            # --------------------------------------------------------
            try:
                details = self.ib.reqContractDetails(contract)
                if not details:
                    print(f"❌ IB cannot identify contract for ticker {ticker}. Skipping.")
                    logger.warning(f"Invalid contract for {ticker}. Skipping.")
                    continue
            except Exception as e:
                print(f"❌ Error requesting contract details for {ticker}: {e}")
                logger.error(f"Contract details error for {ticker}: {e}")
                continue

            # SAFE API CALL
            try:
                bars = safe_req(contract)
            except Exception as e:
                logger.error(f"Error fetching ticker {ticker}: {e}")
                print(f"❌ Error fetching IB data for {ticker}: {e}")
                continue

            df = util.df(bars)

            # Handle edge cases
            if df.empty or "date" not in df.columns or "close" not in df.columns:
                logger.warning(f"No IB data returned for {ticker}.")
                print(f"⚠️ WARNING: No data returned for {ticker}. Skipping.")
                continue

            ib_prices_dct[ticker] = df.loc[:,['date', 'close']].set_index('date').rename(columns={'close':ticker})

            # ANTI-RATE-LIMIT SLEEP
            time.sleep(11)

        # --------------------------------------------------------
        # 5. Format into a single dataframe
        # --------------------------------------------------------
        self.universe_prices_ib = self.format_ib_historical_prices(ib_prices_dct=ib_prices_dct)
        self.universe_prices_ib.sort_index(inplace=True)

        # --------------------------------------------------------
        # 6. Save results
        # --------------------------------------------------------
        if save_prices:
            with open(r'.\data\ib_historical_prices_dct.pkl', 'wb') as f:
                pickle.dump(ib_prices_dct, f)
            self.universe_prices_ib.to_csv(r'.\data\ib_historical_prices.csv',
                                           index=True)
            logger.info("Saved historical prices to disk.")

        logger.info("Finished IB historical fetch successfully.")
        # --------------------------------------------------------
        # 7. Optionally return a value
        # --------------------------------------------------------
        if return_bool:
            return self.universe_prices_ib


    # async def fetch_one_ticker(
    #         self,
    #         ticker,
    #         wrds_df,
    #         end_date,
    #         past_period,
    #         frequency,
    #         data_prices,
    #         use_rth,
    #         format_date,
    #         semaphore,
    #         retries=3
    # ):
    #     """
    #     Async helper to fetch a single ticker from IB with:
    #     - Semaphore to limit concurrency
    #     - Retry logic
    #     - IB pacing-friendly sleep
    #     """
    #
    #     subset = wrds_df[wrds_df["ticker"] == ticker]
    #     if subset.empty:
    #         logger.warning(f"{ticker} not found in WRDS universe. Skipping.")
    #         return None
    #
    #     exchange_ib = subset.iloc[0]["exchange_ib"]
    #     currency = subset.iloc[0]["currency"]
    #
    #     contract = Stock(symbol=ticker, exchange=exchange_ib, currency=currency)
    #
    #     async with semaphore:
    #         for attempt in range(1, retries + 1):
    #             try:
    #                 bars = await self.ib.reqHistoricalDataAsync(
    #                     contract=contract,
    #                     endDateTime=end_date,
    #                     durationStr=past_period,
    #                     barSizeSetting=frequency,
    #                     whatToShow=data_prices,
    #                     useRTH=use_rth,
    #                     formatDate=format_date
    #                 )
    #
    #                 # IB pacing rule: pause between calls
    #                 await asyncio.sleep(1.1)
    #
    #                 df = util.df(bars)
    #
    #                 if df.empty or "date" not in df.columns or "close" not in df.columns:
    #                     logger.warning(f"No data returned for {ticker}.")
    #                     return None
    #
    #                 return df.loc[:, ["date", "close"]] \
    #                     .set_index("date") \
    #                     .rename(columns={"close": ticker})
    #
    #             except Exception as e:
    #                 logger.error(f"[{ticker}] Attempt {attempt}/{retries} failed: {e}")
    #                 await asyncio.sleep(2 + attempt)  # exponential backoff
    #
    #         logger.error(f"[{ticker}] Failed after {retries} retries.")
    #         return None
    #
    # async def fetch_ib_historical_prices(
    #         self,
    #         end_date='',
    #         past_period='1 M',
    #         frequency='1 day',
    #         data_prices='ADJUSTED_LAST',
    #         use_rth=True,
    #         format_date=1,
    #         save_prices=True,
    #         return_bool=False
    # ):
    #
    #     logger.info("Starting ASYNC IB historical fetch with rate limiting...")
    #
    #     # ---------------------------------------
    #     # 0. Ensure IB connection
    #     # ---------------------------------------
    #     if self.ib is None:
    #         self.connect_ib()
    #
    #     # ---------------------------------------
    #     # 1. Load WRDS universe
    #     # ---------------------------------------
    #     if self.wrds_universe is None:
    #         file_path = r".\data\wrds_universe.csv"
    #         self.wrds_universe = pd.read_csv(
    #             file_path, index_col="date", parse_dates=["date"]
    #         )
    #
    #     # ---------------------------------------
    #     # 2. Load tickers list
    #     # ---------------------------------------
    #     if self.tickers_across_dates is None:
    #         file_path = r".\data\tickers_across_dates_short.pkl"
    #         with open(file_path, "rb") as f:
    #             self.tickers_across_dates = pickle.load(f)
    #
    #     wrds_df = self.wrds_universe.sort_index(ascending=False)
    #
    #     # ---------------------------------------
    #     # 3. Create rate-limiter + concurrency control
    #     # ---------------------------------------
    #     max_concurrent_requests = 5  # IB safe limit
    #     semaphore = asyncio.Semaphore(max_concurrent_requests)
    #
    #     # ---------------------------------------
    #     # 4. Create async tasks
    #     # ---------------------------------------
    #     tasks = [
    #         self.fetch_one_ticker(
    #             ticker=t,
    #             wrds_df=wrds_df,
    #             end_date=end_date,
    #             past_period=past_period,
    #             frequency=frequency,
    #             data_prices=data_prices,
    #             use_rth=use_rth,
    #             format_date=format_date,
    #             semaphore=semaphore
    #         )
    #         for t in self.tickers_across_dates
    #     ]
    #
    #     # ---------------------------------------
    #     # 5. Run with async progress bar
    #     # ---------------------------------------
    #     results = []
    #     for result in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Fetching IB Data"):
    #         results.append(await result)
    #
    #     # ---------------------------------------
    #     # 6. Collect results into dict
    #     # ---------------------------------------
    #     ib_prices_dct = {
    #         ticker: df
    #         for ticker, df in zip(self.tickers_across_dates, results)
    #         if df is not None
    #     }
    #
    #     logger.info(f"{len(ib_prices_dct)} tickers fetched successfully.")
    #
    #     # ---------------------------------------
    #     # 7. Combine into final DataFrame
    #     # ---------------------------------------
    #     self.universe_prices_ib = pd.concat(ib_prices_dct.values(), axis=1, join="outer")
    #
    #     # ---------------------------------------
    #     # 8. Save
    #     # ---------------------------------------
    #     if save_prices:
    #         with open(r".\data\ib_historical_prices_dct.pkl", "wb") as f:
    #             pickle.dump(ib_prices_dct, f)
    #
    #         self.universe_prices_ib.to_csv(r".\data\ib_historical_prices.csv")
    #         logger.info("IB historical prices saved.")
    #
    #     if return_bool:
    #         return self.universe_prices_ib

    @staticmethod
    def format_ib_historical_prices(ib_prices_dct:dict)->pd.DataFrame:
        """ Given a dict of DataFrames from IB, format them into a single DataFrame."""
        aligned_df = pd.concat(
            ib_prices_dct.values(),
            axis=1,
            join="outer"  # forces union of all dates
        )
        return aligned_df












