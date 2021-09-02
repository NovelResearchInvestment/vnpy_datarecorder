""""""
import threading
import traceback
import importlib
import os, sys
from threading import Thread
from queue import Queue, Empty
from copy import copy

from collections import defaultdict
from pathlib import Path
from typing import Any, Callable
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from tzlocal import get_localzone
from glob import glob

from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
# from vnpy.trader.database import database_manager
from vnpy.trader.database import BaseDatabase
from vnpy.trader.constant import Exchange
from vnpy.trader.object import (
    OrderRequest,
    SubscribeRequest,
    HistoryRequest,
    LogData,
    TickData,
    BarData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData
)

from vnpy.trader.event import (
    EVENT_TIMER, EVENT_TICK, EVENT_CONTRACT,
    EVENT_POSITION, EVENT_ORDER, EVENT_TRADE
)
from vnpy.trader.utility import load_json, save_json, BarGenerator
from vnpy.trader.converter import OffsetConverter

from vnpy.trader.constant import Direction, Offset, OrderType, Interval

from .base import (
    APP_NAME,
    EVENT_RECORDER_LOG,
    EVENT_RECORDER_UPDATE,
    EVENT_RECORDER_EXCEPTION,
    EngineType,
    StopOrder,
)
from .template import DataTemplate


class DataRecorderEngine(BaseEngine):
    """"""
    # setting_filename = "data_recorder_setting.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.data_recorder_setting = self.main_engine.global_setting['data_recorder_setting.file']

        self.queue = Queue()
        self.thread = Thread(target=self.run)

        self.active = False

        self.tick_recordings = {}
        self.bar_recordings = {}
        self.bar_generators = {}

        self.timer_count = 0
        self.timer_interval = 10

        self.ticks = defaultdict(list)
        self.bars = defaultdict(list)
        self.orders = defaultdict(list)
        self.trades = defaultdict(list)
        self.positions = defaultdict(list)
        self.accounts = defaultdict(list)

        # driver = self.main_engine.global_setting['database.driver']
        # self.database_manager = importlib.import_module(f"vnpy.database.{driver}").database_manager

        module_name = self.main_engine.global_setting['database.module']
        driver = self.main_engine.global_setting['database.driver']

        try:
            # database_module: BaseDatabase = importlib.import_module(f"vnpy_{driver}.{driver}_database")
            # self.database_manager = eval(f"database_module.{driver.capitalize()}Database")
            self.database_manager = importlib.import_module(f"vnpy_{driver}.{driver}_database").database_manager
        except ModuleNotFoundError:
            print(f"找不到数据库驱动{module_name}，使用默认的SQLite数据库")
            self.database_manager: BaseDatabase = importlib.import_module("vnpy.database.sqlite").database_manager

        self.load_setting()
        self.register_event()
        self.start()
        self.put_event()

    def load_setting(self):
        """"""
        self.data_setting = load_json(self.data_recorder_setting)
        self.tick_recordings = self.data_setting.get("tick", {})
        self.bar_recordings = self.data_setting.get("bar", {})
        self.data_setting.get("location", "")


    def save_setting(self):
        """"""
        setting = {
            "tick": self.tick_recordings,
            "bar": self.bar_recordings
        }
        save_json(self.setting_filename + '.cache', setting)

    def run(self):
        """"""
        while self.active:
            try:
                task = self.queue.get(timeout=1)
                task_type, data = task

                try:
                    self.write_log(data)
                except Exception as e:
                    print(e)

                if task_type == "tick":
                    self.database_manager.save_tick_data(data)
                    self.write_log(data)
                elif task_type == "bar":
                    self.database_manager.save_bar_data(data)
                    self.write_log(data)
                elif task_type == "order":
                    self.database_manager.save_order_data(data)
                    self.write_log(data)
                elif task_type == "trade":
                    self.database_manager.save_trade_data(data)
                    self.write_log(data)
                elif task_type == "position":
                    self.database_manager.save_position_data(data)
                    self.write_log(data)
                elif task_type == "account":
                    self.database_manager.save_account_data(data)
                    self.write_log(data)
                else:
                    raise ValueError(f"unknown task_type: {task_type}")

            except Empty:
                print()
                self.write_log(f"Thread queue is waiting...[{self.queue.qsize()}, {self.active}, {self.thread.is_alive()}, {self.thread._is_stopped}]")
                continue
                # if self.active and self.thread.is_alive():
                #     continue
                # else:
                #     raise Exception("engine thread is not alive.")

            except Exception as e:
                print(e)
                self.active = False
                info = sys.exc_info()
                event = Event(EVENT_RECORDER_EXCEPTION, info)
                self.event_engine.put(event)

    def close(self):
        """"""
        self.active = False

        if self.thread.isAlive():
            self.thread.join()

    def start(self):
        """"""
        self.active = True
        if self.thread.isAlive():
            self.thread.start()

    def add_bar_recording(self, vt_symbol: str):
        """"""
        if vt_symbol in self.bar_recordings:
            self.write_log(f"已在K线记录列表中：{vt_symbol}")
            return

        if Exchange.LOCAL.value not in vt_symbol:
            contract = self.main_engine.get_contract(vt_symbol)
            if not contract:
                self.write_log(f"找不到合约：{vt_symbol}")
                return

            self.bar_recordings[vt_symbol] = {
                "symbol": contract.symbol,
                "exchange": contract.exchange.value,
                "gateway_name": contract.gateway_name
            }

            self.subscribe(contract)
        else:
            self.bar_recordings[vt_symbol] = {}

        self.save_setting()
        self.put_event()

        self.write_log(f"添加K线记录成功：{vt_symbol}")

    def add_tick_recording(self, vt_symbol: str):
        """"""
        if vt_symbol in self.tick_recordings:
            self.write_log(f"已在Tick记录列表中：{vt_symbol}")
            return

        # For normal contract
        if Exchange.LOCAL.value not in vt_symbol:
            contract = self.main_engine.get_contract(vt_symbol)
            if not contract:
                self.write_log(f"找不到合约：{vt_symbol}")
                return

            self.tick_recordings[vt_symbol] = {
                "symbol": contract.symbol,
                "exchange": contract.exchange.value,
                "gateway_name": contract.gateway_name
            }

            self.subscribe(contract)
        # No need to subscribe for spread data
        else:
            self.tick_recordings[vt_symbol] = {}

        self.save_setting()
        self.put_event()

        self.write_log(f"添加Tick记录成功：{vt_symbol}")

    def remove_bar_recording(self, vt_symbol: str):
        """"""
        if vt_symbol not in self.bar_recordings:
            self.write_log(f"不在K线记录列表中：{vt_symbol}")
            return

        self.bar_recordings.pop(vt_symbol)
        self.save_setting()
        self.put_event()

        self.write_log(f"移除K线记录成功：{vt_symbol}")

    def remove_tick_recording(self, vt_symbol: str):
        """"""
        if vt_symbol not in self.tick_recordings:
            self.write_log(f"不在Tick记录列表中：{vt_symbol}")
            return

        self.tick_recordings.pop(vt_symbol)
        self.save_setting()
        self.put_event()

        self.write_log(f"移除Tick记录成功：{vt_symbol}")

    def register_event(self, events: list = [EVENT_TICK, EVENT_CONTRACT, EVENT_ORDER, EVENT_TRADE, EVENT_POSITION]):
        """"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

        if EVENT_TICK in events:
            self.event_engine.register(EVENT_TICK, self.process_tick_event)

        if EVENT_CONTRACT in events:
            self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)

        if EVENT_ORDER in events:
            self.event_engine.register(EVENT_ORDER, self.process_order_event)

        if EVENT_TRADE in events:
            self.event_engine.register(EVENT_TRADE, self.process_trade_event)

        if EVENT_POSITION in events:
            self.event_engine.register(EVENT_POSITION, self.process_position_event)

    def update_tick(self, tick: TickData):
        """"""
        if tick.vt_symbol in self.tick_recordings:
            self.record_tick(copy(tick))

        if tick.vt_symbol in self.bar_recordings:
            bg = self.get_bar_generator(tick.vt_symbol)
            bg.update_tick(copy(tick))

    def process_timer_event(self, event: Event):
        """"""
        self.timer_count += 1

        # if self.timer_count < self.timer_interval:
        #     return
        self.timer_count = 0

        for bars in self.bars.values():
            self.queue.put(("bar", bars))
        self.bars.clear()

        for ticks in self.ticks.values():
            self.queue.put(("tick", ticks))
        self.ticks.clear()

        for orders in self.orders.values():
            self.queue.put(("order", orders))
        self.orders.clear()

        for trades in self.trades.values():
            self.queue.put(("trade", trades))
        self.trades.clear()

        for positions in self.positions.values():
            self.write_log(f"position: {len(self.positions.values())}")
            self.queue.put(("position", positions))
        self.positions.clear()

        for accounts in self.accounts.values():
            self.write_log(f"accounts: {len(self.accounts.values())}")
            self.queue.put(("account", accounts))
        self.accounts.clear()

    def process_tick_event(self, event: Event):
        """"""
        tick = event.data
        self.update_tick(tick)

    def process_contract_event(self, event: Event):
        """"""
        contract = event.data
        vt_symbol = contract.vt_symbol

        if (vt_symbol in self.tick_recordings or vt_symbol in self.bar_recordings):
            self.subscribe(contract)

    def process_order_event(self, event: Event):
        """"""
        order = event.data
        self.record_order(copy(order))

    def process_trade_event(self, event: Event):
        """"""
        trade = event.data
        self.record_trade(copy(trade))

    def process_position_event(self, event: Event):
        """"""
        position = event.data
        self.record_position(copy(position))

    def process_account_event(self, event: Event):
        """
        收到账户事件推送
        """
        account = event.data
        self.record_account(copy(account))

    def write_log(self, msg: str, strategy: DataTemplate = None):
        """
        Create engine log event.
        """
        if strategy:
            msg = f"[{strategy.strategy_name}]  {msg}"

        log = LogData(msg=msg, gateway_name=APP_NAME)
        event = Event(type=EVENT_RECORDER_LOG, data=log)
        self.event_engine.put(event)

    def put_event(self):
        """"""
        tick_symbols = list(self.tick_recordings.keys())
        tick_symbols.sort()

        bar_symbols = list(self.bar_recordings.keys())
        bar_symbols.sort()

        data = {
            "tick": tick_symbols,
            "bar": bar_symbols
        }

        event = Event(
            EVENT_RECORDER_UPDATE,
            data
        )
        self.event_engine.put(event)

    def record_tick(self, tick: TickData):
        """"""
        self.ticks[tick.vt_symbol].append(tick)

    def record_bar(self, bar: BarData):
        """"""
        self.bars[bar.vt_symbol].append(bar)

    def record_order(self, order: OrderData):
        self.orders[order.vt_symbol].append(order)

    def record_trade(self, trade: TradeData):
        self.trades[trade.vt_symbol].append(trade)

    def record_position(self, position: PositionData):
        self.positions[position.vt_symbol].append(position)

    def record_account(self, account: AccountData):
        self.accounts[account.vt_symbol].append(account)


    def get_bar_generator(self, vt_symbol: str):
        """"""
        bg = self.bar_generators.get(vt_symbol, None)

        if not bg:
            bg = BarGenerator(self.record_bar)
            self.bar_generators[vt_symbol] = bg

        return bg

    def subscribe(self, contract: ContractData):
        """"""
        req = SubscribeRequest(
            symbol=contract.symbol,
            exchange=contract.exchange
        )
        self.write_log(f"发出订阅 {contract.vt_symbol} 请求")
        self.main_engine.subscribe(req, contract.gateway_name)
