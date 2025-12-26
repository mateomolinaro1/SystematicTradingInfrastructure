from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from typing import Union


class WeightingScheme(ABC):
    """Abstract class to define the interface for the weighting scheme"""

    def __init__(self, returns: pd.DataFrame, aligned_returns:pd.DataFrame, signals: pd.DataFrame, rebal_periods: Union[int, None],
                 portfolio_type: str = "long_only"):
        self.returns = returns
        self.aligned_returns = aligned_returns
        self.signals = signals
        if not isinstance(rebal_periods, (int, type(None))):
            raise ValueError("rebal_periods must be an int or None.")
        self.rebal_periods = rebal_periods
        self.portfolio_type = portfolio_type
        self.weights = None
        self.rebalanced_weights = None
        self.returns_after_fees = None
        self.turnover = None

    @abstractmethod
    def compute_weights(self):
        """Compute the weights for the strategy"""
        pass

    @abstractmethod
    def rebalance_portfolio(self, transaction_cost_bp: float = 10):
        """
        Rebalance the portfolio at the specified frequency and computes transaction costs

        Parameters:
        - transaction_cost_bp (float): Transaction costs in basis points (default: 10bp)

        Returns:
        - tuple: (rebalanced_weights, net_returns)
        """
        pass


class EqualWeightingScheme(WeightingScheme):
    """Class to implement the equal weighting scheme"""

    def __init__(self, returns: pd.DataFrame, aligned_returns:pd.DataFrame, signals: pd.DataFrame, rebal_periods: int = 0,
                 portfolio_type: str = "long_only"):
        super().__init__(returns, aligned_returns, signals, rebal_periods, portfolio_type)

    def compute_weights(
            self,
            return_bool:bool=False
    )->Union[None, pd.DataFrame]:
        """Compute the weights for the equal weighting scheme"""
        number_of_positive_signals = (self.signals == 1).sum(axis=1)
        number_of_negative_signals = (self.signals == -1).sum(axis=1)

        weights = pd.DataFrame(data=np.nan, index=self.signals.index, columns=self.signals.columns)

        if self.portfolio_type == "long_only":
            # Check that there is at least one positive signal at each date
            if (number_of_positive_signals > 0).sum() == number_of_positive_signals.shape[0]:
                weights[self.signals == 1] = self.signals[self.signals == 1].div(number_of_positive_signals, axis=0)
            else:
                # Compute weights where there is at least one positive signal on the row
                mask_valid = number_of_positive_signals > 0
                weights[self.signals == 1] = self.signals[self.signals == 1].div(
                    number_of_positive_signals.where(mask_valid), axis=0)

                # Setting weights to nan at rows where all signals are missing
                weights.loc[~mask_valid, :] = np.nan  # Optional as in the creation of weights, default data set to 0
                print("For some dates, there is no signals. Weights set to nan by default.")

        elif self.portfolio_type == "short_only":
            # Check that there is at least one negative signal at each date
            if (number_of_negative_signals > 0).sum() == number_of_negative_signals.shape[0]:
                weights[self.signals == -1] = self.signals[self.signals == -1].div(number_of_negative_signals, axis=0)
            else:
                # Compute weights where there is at least one negative signal on the row
                mask_valid = number_of_negative_signals > 0
                weights[self.signals == -1] = self.signals[self.signals == -1].div(
                    number_of_negative_signals.where(mask_valid), axis=0)

                # Setting weights to 0 at rows where all signals are missing
                weights.loc[~mask_valid, :] = np.nan  # Optional as in the creation of weights, default data set to 0
                print("For some dates, there is no signals. Weights set to nan by default.")

        elif self.portfolio_type == "long_short":
            # Check that there is at least one positive and one negative signal at each date
            if ((number_of_positive_signals > 0).sum() == number_of_positive_signals.shape[0] and
                    (number_of_negative_signals > 0).sum() == number_of_negative_signals.shape[0]):
                weights[self.signals == 1] = self.signals[self.signals == 1].div(number_of_positive_signals, axis=0)
                weights[self.signals == -1] = self.signals[self.signals == -1].div(number_of_negative_signals, axis=0)
            else:
                raise ValueError("Error division by 0. There is no positive or negative signal for some dates.")

        else:
            raise ValueError("portfolio_type not supported")

        self.weights=weights
        if return_bool:
            return self.weights

    def rebalance_portfolio(
            self,
            return_bool:bool=False
    )->Union[None, pd.DataFrame]:
        """
        Rebalance the portfolio and compute weights evolution between rebalancing dates.
        Returns rebalanced weights and net returns (after transaction costs).

        Parameters:
        - return_bool: return the df is set to true otherwise just stored

        Returns:
        - None or df: rebalanced_weights
        """
        # First compute weights "not yet with drifts" if not already done
        if self.weights is None:
            self.weights = self.compute_weights()

        # If no rebalancing period specified or rebalancing at every period, return original weights
        if self.rebal_periods is None or self.rebal_periods == 0:
            if return_bool:
                return self.weights

        if not isinstance(self.weights.index, pd.DatetimeIndex):
            raise ValueError("The index must be of type pd.DatetimeIndex")

        # If rebalancing period specified, we must compute weights accounting for drift
        # Step 0 Initialize the rebalanced_weights df
        self.rebalanced_weights = self.weights.copy()
        self.turnover = pd.DataFrame(data=np.nan, index=self.weights.index, columns=["turnover"])

        # Step 1 define the rebalancing dates range
        flg = self.weights.sum(min_count=1, axis=1)
        flg = pd.notna(flg)
        dates = list(self.weights.index[flg])
        start_date = dates[0]
        rebal_dates = dates[::self.rebal_periods]
        rebal_dates.pop(0)

        # Step 2: loop on all the dates
        for date in self.rebalanced_weights.index:
            if date < start_date:
                # we do nothing as it is already set to nan
                continue
            elif ((date>start_date) and (date in rebal_dates)) or date==start_date:
                # we do nothing because being at a rebalancing date means that we "reset" the weights
                # to EW and self.computes weights does that to every dates by default

                # Compute turnover
                num_idx = self.rebalanced_weights.index.get_loc(date)
                turnover = (
                    (self.rebalanced_weights.iloc[num_idx, :].sub(
                        self.rebalanced_weights.iloc[num_idx - 1, :],
                        fill_value=0.0)
                    )
                    .abs()
                    .sum()
                )
                self.turnover.loc[date] = turnover

            elif (date>start_date) and (date not in rebal_dates):
                # If we are not at a rebalancing date, we must derive the weights in between rebal dates
                num_idx = self.rebalanced_weights.index.get_loc(date)
                prev_weights = self.rebalanced_weights.iloc[num_idx-1,:]
                aligned_ret = self.returns.iloc[num_idx,:]
                drifted_w = prev_weights*(1+aligned_ret)
                drifted_w = drifted_w/(drifted_w.sum())
                self.rebalanced_weights.loc[date,:] = drifted_w

            else:
                continue

class NaiveRiskParity(WeightingScheme):
    """Class to implement the naive risk parity weighting scheme"""

    def __init__(self, returns: pd.DataFrame, signals: pd.DataFrame, rebal_periods: int = 0,
                 portfolio_type: str = "long_only", vol_lookback: int = 252):
        """
        Initialize the naive risk parity model.

        Parameters:
        - returns (pd.DataFrame): DataFrame of asset returns.
        - signals (pd.DataFrame): DataFrame of trading signals (1 for long, -1 for short, 0 for neutral).
        - rebal_periods (int): Rebalancing frequency (default: 0 for daily).
        - portfolio_type (str): "long_only", "short_only", or "long_short".
        - vol_lookback (int): Lookback period for volatility calculation (default: 252 days ~ 1 year).
        """
        super().__init__(returns, signals, rebal_periods, portfolio_type)
        self.vol_lookback = vol_lookback

    def compute_weights(self):
        """Compute weights using the inverse volatility method."""
        rolling_vol = self.returns.rolling(window=self.vol_lookback, min_periods=21).std()
        inv_vol = 1 / rolling_vol  # Inverse of volatility
        inv_vol.replace([np.inf, -np.inf], np.nan, inplace=True)  # Handle division by zero
        inv_vol = inv_vol.fillna(0)  # Replace NaN values

        # Mask to apply signals
        number_of_positive_signals = (self.signals == 1).sum(axis=1)
        number_of_negative_signals = (self.signals == -1).sum(axis=1)

        weights = pd.DataFrame(data=0.0, index=self.signals.index, columns=self.signals.columns)

        if self.portfolio_type == "long_only":
            valid_mask = number_of_positive_signals > 0
            masked_inv_vol = inv_vol * (self.signals == 1)
            weights = masked_inv_vol.div(masked_inv_vol.sum(axis=1), axis=0)
            weights.loc[~valid_mask, :] = 0  # Set weights to 0 if no valid signals

        elif self.portfolio_type == "short_only":
            valid_mask = number_of_negative_signals > 0
            masked_inv_vol = inv_vol * (self.signals == -1)
            weights = masked_inv_vol.div(masked_inv_vol.sum(axis=1), axis=0)
            weights.loc[~valid_mask, :] = 0

        elif self.portfolio_type == "long_short":
            valid_mask = (number_of_positive_signals > 0) & (number_of_negative_signals > 0)
            long_weights = inv_vol * (self.signals == 1)
            short_weights = inv_vol * (self.signals == -1)

            long_weights = long_weights.div(long_weights.sum(axis=1), axis=0)
            short_weights = short_weights.div(short_weights.sum(axis=1), axis=0)

            weights = long_weights - short_weights
            weights.loc[~valid_mask, :] = 0

        else:
            raise ValueError("portfolio_type not supported")

        self.weights = weights.fillna(0)
        return self.weights

    def rebalance_portfolio(self, transaction_cost_bp: float = 10):
        """
        Rebalance the portfolio and compute weights evolution between rebalancing dates.
        Returns rebalanced weights and net returns (after transaction costs).
        """
        if self.weights is None:
            self.weights = self.compute_weights()

        # If no rebalancing period specified or rebalancing at every period, return original weights
        if self.rebal_periods is None or self.rebal_periods == 0:
            strategy_returns = (self.weights * self.returns).sum(axis=1)
            return self.weights, strategy_returns

        # Initialize rebalanced weights and returns
        self.rebalanced_weights = pd.DataFrame(0.0, index=self.weights.index, columns=self.weights.columns)
        strategy_returns = pd.Series(0.0, index=self.weights.index)
        cost = transaction_cost_bp / 10000

        # Set initial weights
        first_date = self.weights.index[0]
        self.rebalanced_weights.loc[first_date] = self.weights.loc[first_date]

        # Create rebalancing dates
        all_dates = self.weights.index
        rebalancing_dates = all_dates[::self.rebal_periods]

        for t, date in enumerate(all_dates[1:], 1):
            prev_date = all_dates[t - 1]
            prev_weights = self.rebalanced_weights.loc[prev_date]

            if date in rebalancing_dates:
                new_weights = self.weights.loc[date]
                turnover = abs(new_weights - prev_weights).sum()
                transaction_cost = turnover * cost

                self.rebalanced_weights.loc[date] = new_weights
                strategy_returns.loc[date] = (prev_weights * self.returns.loc[date]).sum() - transaction_cost
            else:
                drifted_weights = prev_weights * (1 + self.returns.loc[date])
                if self.portfolio_type == "long_only":
                    total_weight = drifted_weights.sum()
                    if total_weight > 0:
                        drifted_weights = drifted_weights / total_weight

                self.rebalanced_weights.loc[date] = drifted_weights
                strategy_returns.loc[date] = (prev_weights * self.returns.loc[date]).sum()

        return self.rebalanced_weights.fillna(0.0), strategy_returns