# -*- coding: utf-8 -*-
"""
Trading Engine
Orchestrates the entire trading system - market data, signals, orders, positions
"""

import threading
import time
from datetime import datetime
from core.authentication import AuthenticationManager
from core.market_data import MarketDataHandler
from core.order_manager import OrderManager
from core.position_tracker import PositionTracker
from strategy.momentum_strategy import MomentumStrategy
from risk_management.risk_manager import RiskManager
from utils.logger import TradingLogger
from utils.market_hours import MarketHours
from config.trading_config import TradingConfig


class TradingEngine:
    """
    Main trading engine that orchestrates all components.
    Manages authentication, market data, signals, orders, and positions.
    """

    def __init__(self, api_key, api_secret):
        """
        Initialize trading engine.
        
        Args:
            api_key: str - Kite API key
            api_secret: str - Kite API secret
        """
        self.logger = TradingLogger.get_logger()
        self.logger.info("=" * 50)
        self.logger.info("TRADING ENGINE INITIALIZED")
        self.logger.info("=" * 50)
        
        # Core components
        self.auth = AuthenticationManager(api_key, api_secret)
        self.kite = None
        self.market_data = None
        self.order_manager = None
        self.position_tracker = None
        self.strategy = None
        self.risk_manager = RiskManager()
        
        # State
        self.is_running = False
        self.is_trading = False
        self.engine_thread = None
        self.symbols_to_trade = []
        self.historical_data_cache = {}  # Cache for historical data
        
        # Callbacks
        self.on_signal = None
        self.on_order = None
        self.on_position = None

    def authenticate(self, access_token=None):
        """
        Authenticate with Kite Connect.
        
        Args:
            access_token: str - Saved access token (optional)
            
        Returns:
            bool - Authentication success
        """
        try:
            if access_token:
                self.kite = self.auth.restore_session(access_token)
                self.logger.info("Session restored from saved token")
            else:
                # Try to load saved session
                saved_session = self.auth.load_saved_session()
                if saved_session:
                    self.kite = self.auth.restore_session(saved_session["access_token"])
                    self.logger.info("Session restored from saved file")
                else:
                    # Need new login
                    login_url = self.auth.get_login_url()
                    self.logger.info(f"Please login: {login_url}")
                    request_token = input("Enter request_token from redirect URL: ")
                    session_data = self.auth.create_session(request_token)
                    self.kite = self.auth.get_kite_instance()
                    self.logger.info(f"New session created for {session_data['user_name']}")
            
            # Initialize other components with authenticated Kite instance
            self.market_data = MarketDataHandler(
                self.kite,
                self.auth.api_key,
                self.kite.access_token
            )
            self.order_manager = OrderManager(self.kite)
            self.position_tracker = PositionTracker(self.kite)
            self.strategy = MomentumStrategy(TradingConfig.STRATEGY_CONFIG["momentum"])
            
            # Fetch user profile to verify authentication
            profile = self.kite.profile()
            self.logger.info(f"Authenticated as: {profile['user_name']} ({profile['email']})")
            
            return True
        
        except Exception as e:
            self.logger.error(f"Authentication failed: {str(e)}")
            return False

    def set_instruments(self, symbols):
        """
        Set instruments to trade.
        
        Args:
            symbols: list - Trading symbols (e.g., ['INFY', 'TCS'])
        """
        self.symbols_to_trade = symbols
        self.logger.info(f"Instruments set: {', '.join(symbols)}")

    def start(self, threaded=True):
        """
        Start the trading engine.
        
        Args:
            threaded: bool - Run in separate thread
        """
        if not self.kite:
            self.logger.error("Not authenticated. Call authenticate() first.")
            return False
        
        if not self.symbols_to_trade:
            self.logger.error("No instruments set. Call set_instruments() first.")
            return False
        
        # Check market status
        market_status = MarketHours.get_market_status()
        if not market_status["is_open"]:
            self.logger.warning(f"Market is not open. Next close: {market_status['market_close']}")
        
        try:
            # Connect to market data
            self.logger.info("Connecting to market data...")
            self.market_data.connect(threaded=True)
            time.sleep(1)
            
            # Fetch initial historical data
            self.logger.info("Fetching historical data...")
            self._fetch_historical_data()
            
            self.is_running = True
            self.is_trading = True
            
            if threaded:
                self.engine_thread = threading.Thread(target=self._run_engine, daemon=True)
                self.engine_thread.start()
                self.logger.info("Trading engine started (threaded mode)")
            else:
                self._run_engine()
                self.logger.info("Trading engine started (blocking mode)")
            
            return True
        
        except Exception as e:
            self.logger.error(f"Failed to start engine: {str(e)}")
            return False

    def _run_engine(self):
        """
        Main engine loop - monitors market and generates signals.
        """
        try:
            while self.is_running:
                # Check market hours
                market_status = MarketHours.get_market_status()
                
                if not market_status["is_open"]:
                    # Market closed
                    time_to_close = market_status["time_to_close"]
                    if time_to_close:
                        self.logger.info(f"Market closes in {time_to_close}")
                    self.is_trading = False
                    time.sleep(60)
                    continue
                
                self.is_trading = True
                
                # Process each symbol
                for symbol in self.symbols_to_trade:
                    try:
                        self._process_symbol(symbol)
                    except Exception as e:
                        self.logger.error(f"Error processing {symbol}: {str(e)}")
                
                # Sleep before next cycle
                time.sleep(5)  # 5-second cycle
        
        except Exception as e:
            self.logger.error(f"Engine error: {str(e)}")
            self.stop()

    def _process_symbol(self, symbol):
        """
        Process a single symbol - fetch data, generate signal, manage positions.
        
        Args:
            symbol: str - Trading symbol
        """
        # Get current price
        exchange = "NSE" if TradingConfig.MARKET.value == "nse" else "MCX"
        
        # Try to get from market data
        current_price = self.market_data.get_ltp(symbol) if self.market_data else None
        
        if not current_price:
            # Fetch quote from API
            try:
                quote = self.kite.quote(f"{exchange}:{symbol}")
                if quote and exchange in quote:
                    current_price = quote[exchange][symbol].get("last_price")
            except:
                pass
        
        if not current_price:
            return
        
        # Get historical data
        if symbol not in self.historical_data_cache:
            self._fetch_symbol_historical_data(symbol)
        
        historical_data = self.historical_data_cache.get(symbol, [])
        
        if not historical_data:
            return
        
        # Generate signal
        price_data = {"ltp": current_price}
        signal = self.strategy.generate_signal(symbol, price_data, historical_data)
        
        # Store signal
        self.strategy.update_signal(symbol, signal)
        
        # Trigger callback
        if self.on_signal:
            self.on_signal(signal)
        
        # Check for position
        position = self.position_tracker.get_position(symbol)
        
        if position and position["status"] == "OPEN":
            # Update position P&L
            updated_position = self.position_tracker.update_position(symbol, current_price)
            
            # Check exit conditions
            self._check_exit_conditions(symbol, current_price, position)
        
        else:
            # No open position - check for entry signal
            if self.strategy.validate_signal(signal):
                self._attempt_entry(symbol, signal, current_price)

    def _attempt_entry(self, symbol, signal, current_price):
        """
        Attempt to enter a position based on signal.
        
        Args:
            symbol: str - Trading symbol
            signal: dict - Trading signal
            current_price: float - Current price
        """
        try:
            # Check if can enter
            can_enter, reason = self.risk_manager.can_enter_position(symbol)
            if not can_enter:
                self.logger.warning(f"Cannot enter {symbol}: {reason}")
                return
            
            # Get entry parameters
            direction = signal["direction"]
            entry_price = current_price
            atr = signal["indicators"].get("atr")
            
            # Calculate stop loss and target
            stop_loss = self.strategy.get_stop_loss(symbol, direction, entry_price, atr)
            target = self.strategy.get_target(symbol, direction, entry_price, stop_loss)
            
            # Calculate position size
            capital = 100000  # TODO: Get from account
            quantity = self.risk_manager.calculate_position_size(
                capital, entry_price, stop_loss
            )
            
            # Validate order
            is_valid, reason = self.risk_manager.validate_order(
                symbol, entry_price, quantity, direction, stop_loss, target
            )
            
            if not is_valid:
                self.logger.warning(f"Order validation failed for {symbol}: {reason}")
                return
            
            # Place entry order
            exchange = "NSE" if TradingConfig.MARKET.value == "nse" else "MCX"
            transaction_type = "BUY" if direction == "BUY" else "SELL"
            
            order_data = self.order_manager.place_order(
                exchange=exchange,
                tradingsymbol=symbol,
                transaction_type=transaction_type,
                quantity=quantity,
                variety="regular",
                product="MIS",
                order_type="MARKET",
                tag=f"MOMENTUM_{direction}"
            )
            
            # Open position
            self.position_tracker.open_position(
                symbol=symbol,
                entry_price=entry_price,
                quantity=quantity,
                side=direction,
                stop_loss=stop_loss,
                target=target,
                order_id=order_data["order_id"]
            )
            
            # Increment active positions
            self.risk_manager.increment_active_positions()
            
            # Trigger callback
            if self.on_position:
                self.on_position("OPEN", self.position_tracker.get_position(symbol))
            
            self.logger.info(
                f"Entry: {symbol} {direction} x{quantity} @ {entry_price} | "
                f"SL: {stop_loss} | Target: {target}"
            )
        
        except Exception as e:
            self.logger.error(f"Entry error for {symbol}: {str(e)}")

    def _check_exit_conditions(self, symbol, current_price, position):
        """
        Check if position should be exited.
        
        Args:
            symbol: str - Trading symbol
            current_price: float - Current price
            position: dict - Position data
        """
        direction = position["side"]
        entry_price = position["entry_price"]
        stop_loss = position["stop_loss"]
        target = position["target"]
        
        exit_reason = None
        
        # Check stop loss
        if direction == "BUY":
            if current_price <= stop_loss:
                exit_reason = "STOPLOSS"
        else:  # SELL
            if current_price >= stop_loss:
                exit_reason = "STOPLOSS"
        
        # Check target
        if direction == "BUY":
            if current_price >= target:
                exit_reason = "TARGET"
        else:  # SELL
            if current_price <= target:
                exit_reason = "TARGET"
        
        # Check market close time
        market_status = MarketHours.get_market_status()
        if market_status["time_to_close"] and market_status["time_to_close"].total_seconds() < 300:
            exit_reason = "MARKET_CLOSE"
        
        if exit_reason:
            self._exit_position(symbol, current_price, exit_reason)

    def _exit_position(self, symbol, exit_price, reason):
        """
        Exit a position.
        
        Args:
            symbol: str - Trading symbol
            exit_price: float - Exit price
            reason: str - Exit reason
        """
        try:
            position = self.position_tracker.get_position(symbol)
            
            if not position:
                return
            
            # Close position
            exchange = "NSE" if TradingConfig.MARKET.value == "nse" else "MCX"
            transaction_type = "SELL" if position["side"] == "BUY" else "BUY"
            
            order_data = self.order_manager.place_order(
                exchange=exchange,
                tradingsymbol=symbol,
                transaction_type=transaction_type,
                quantity=position["quantity"],
                variety="regular",
                product="MIS",
                order_type="MARKET",
                tag=f"EXIT_{reason}"
            )
            
            # Update position
            closed_position = self.position_tracker.close_position(
                symbol, exit_price, order_data["order_id"]
            )
            
            # Decrement active positions
            self.risk_manager.decrement_active_positions()
            
            # Record P&L
            if closed_position["pnl"] < 0:
                self.risk_manager.record_trade_loss(abs(closed_position["pnl"]))
            else:
                self.risk_manager.record_trade_profit(closed_position["pnl"])
            
            # Trigger callback
            if self.on_position:
                self.on_position("CLOSED", closed_position)
            
            self.logger.info(
                f"Exit: {symbol} {position['side']} x{position['quantity']} @ {exit_price} | "
                f"P&L: {closed_position['pnl']:.2f} ({closed_position['pnl_percent']:.2f}%) | "
                f"Reason: {reason}"
            )
        
        except Exception as e:
            self.logger.error(f"Exit error for {symbol}: {str(e)}")

    def _fetch_historical_data(self):
        """
        Fetch historical data for all symbols.
        """
        for symbol in self.symbols_to_trade:
            self._fetch_symbol_historical_data(symbol)

    def _fetch_symbol_historical_data(self, symbol):
        """
        Fetch historical data for a symbol.
        
        Args:
            symbol: str - Trading symbol
        """
        try:
            # TODO: Fetch real historical data via Kite API
            # For now, use dummy data
            self.historical_data_cache[symbol] = self._generate_dummy_historical_data()
        except Exception as e:
            self.logger.error(f"Error fetching historical data for {symbol}: {str(e)}")

    def _generate_dummy_historical_data(self, num_candles=50):
        """
        Generate dummy historical data for testing.
        
        Returns:
            list - OHLC data
        """
        import random
        data = []
        price = 100
        for i in range(num_candles):
            change = random.uniform(-2, 2)
            open_price = price
            close_price = price + change
            high_price = max(open_price, close_price) + random.uniform(0, 1)
            low_price = min(open_price, close_price) - random.uniform(0, 1)
            
            data.append({
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": random.randint(10000, 100000),
            })
            
            price = close_price
        
        return data

    def stop(self):
        """
        Stop the trading engine.
        """
        try:
            self.is_running = False
            self.is_trading = False
            
            # Disconnect market data
            if self.market_data:
                self.market_data.disconnect()
            
            # Close all positions
            for symbol in list(self.position_tracker.get_all_positions().keys()):
                try:
                    current_price = self.market_data.get_ltp(symbol) if self.market_data else 100
                    self._exit_position(symbol, current_price, "MANUAL_STOP")
                except:
                    pass
            
            self.logger.info("Trading engine stopped")
            
            # Print summary
            summary = self.position_tracker.get_position_summary()
            risk_summary = self.risk_manager.get_risk_summary()
            self.logger.info(f"Daily Summary: {summary}")
            self.logger.info(f"Risk Summary: {risk_summary}")
        
        except Exception as e:
            self.logger.error(f"Error stopping engine: {str(e)}")

    def get_status(self):
        """
        Get current engine status.
        
        Returns:
            dict - Status information
        """
        market_status = MarketHours.get_market_status()
        position_summary = self.position_tracker.get_position_summary()
        risk_summary = self.risk_manager.get_risk_summary()
        
        return {
            "is_running": self.is_running,
            "is_trading": self.is_trading,
            "market_status": market_status,
            "positions": position_summary,
            "risk": risk_summary,
            "signals": self.strategy.signals if self.strategy else {},
        }
