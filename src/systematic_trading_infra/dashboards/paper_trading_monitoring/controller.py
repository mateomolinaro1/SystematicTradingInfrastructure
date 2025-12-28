from systematic_trading_infra.dashboards.paper_trading_monitoring.data_loader import (
    load_prices,
    load_orders,
    load_portfolio_value,
    load_weights,
)

class LiveMonitoringController:
    """
    Controller for live paper trading monitoring dashboard.
    Pure orchestration layer.
    """

    def load_all(self) -> dict:
        return {
            "prices": load_prices(),
            "orders": load_orders(),
            "portfolio_value": load_portfolio_value(),
            "weights": load_weights(),
        }
