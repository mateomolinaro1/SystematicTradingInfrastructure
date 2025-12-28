from scripts.update_data import main as update_data
from scripts.update_trading_requirements import main as update_trading_requirements
from scripts.execute_orders import main as execute_orders
from configs.config_update_data import CONFIG as UPDATE_DATA_CONFIG
from configs.config_update_trading_requirements import CONFIG as UPDATE_TR_CONFIG
from configs.config_execute_orders import CONFIG as EXECUTE_ORDERS_CONFIG

def main():
    update_data(**UPDATE_DATA_CONFIG)
    update_trading_requirements(**UPDATE_TR_CONFIG)
    execute_orders(**EXECUTE_ORDERS_CONFIG)


if __name__ == "__main__":
    main()
