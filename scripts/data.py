from config import *
from packages.data_handler import DataHandler
import logging
import asyncio

logging.basicConfig(
    level=logging.INFO,
    filename=r'.\outputs\logger.log',
    filemode="a",
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

dh = DataHandler(wrds_username=WRDS_USERNAME,
                 ib_host=IB_HOST,
                 ib_port=IB_PORT,
                 ib_client_id=IB_CLIENT_ID)
dh.connect_wrds()
dh.connect_ib()
dh.fetch_wrds_historical_universe(wrds_request=WRDS_REQUEST,
                                  fields_wrds_to_keep_for_universe=FIELDS_WRDS_TO_KEEP_FOR_UNIVERSE,
                                  date_cols=DATE_COLS,
                                  saving_config=SAVING_CONFIG_UNIVERSE,
                                  return_bool=RETURN_BOOL_UNIVERSE)
dh.get_wrds_historical_prices(saving_config=SAVING_CONFIG_PRICES,
                              return_bool=RETURN_BOOL_PRICES)
# dh.get_wrds_returns(return_bool=RETURN_BOOL_RETURNS)
dh.logout_wrds()
dh.fetch_ib_historical_prices(end_date=END_DATE,
                              past_period=PAST_PERIOD,
                              frequency=FREQUENCY,
                              data_prices='ADJUSTED_LAST',
                              use_rth=USE_RTH,
                              format_date= FORMAT_DATE,
                              save_prices=SAVE_PRICES,
                              return_bool=RETURN_BOOL_IB_PRICES
                              )

# async def run():
#     dh = DataHandler(wrds_username=WRDS_USERNAME,
#                      ib_host=IB_HOST,
#                      ib_port=IB_PORT,
#                      ib_client_id=IB_CLIENT_ID)
#     await dh.connect_ib()
#
#     await dh.fetch_ib_historical_prices(
#         past_period='1 W',
#         frequency='1 day',
#         return_bool=True
#     )
#
# asyncio.run(run())

# import matplotlib.pyplot as plt
# plt.plot(dh.universe_prices_ib.index, dh.universe_prices_ib)
# plt.show(block=True)