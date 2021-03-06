# The MIT License (MIT)
#
# Copyright (c) 2015-present, Xiaoyou Chen
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from pathlib import Path

from vnpy.trader.app import BaseApp
from vnpy.trader.constant import Direction
from vnpy.trader.object import TickData, BarData, TradeData, OrderData
from vnpy.trader.utility import BarGenerator, ArrayManager

from .base import APP_NAME, StopOrder
from .engine import DataRecorderEngine
from .template import DataTemplate


class DataRecorderApp(BaseApp):
    """"""

    app_name = APP_NAME
    app_module = __module__
    app_path = Path(__file__).parent
    display_name = "DataRecorder"
    engine_class = DataRecorderEngine
    widget_name = "DataRecorderManager"
    icon_name = "datarecorder.ico"



# class AccountRecorderApp(BaseApp):
#     """"""
#
#     app_name = "AccountRecorder"
#     app_module = __module__
#     app_path = Path(__file__).parent
#     display_name = "AccountRecorder"
#     engine_class = AccountRecorderEngine
#     widget_name = "AccountRecorderManager"
#     icon_name = "accountrecorder.ico"
