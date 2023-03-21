#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TMRh20 RF24 interface for Circuitpython rf24l01

SPDX-FileCopyrightText: © 2022 Jan-Jonas Sämann <sprinterfreak@binary-kitchen.de>
SPDX-License-Identifier: GPL-2.0-only
"""

import time
import re
from datetime import datetime
import spidev
import board
from digitalio import DigitalInOut

LIB_RF24 = None


# Use RF24 Library from TMRh20
try:
    if not LIB_RF24:
        from RF24 import RF24, RF24_250KBPS, \
                RF24_PA_MIN, RF24_PA_LOW, RF24_PA_HIGH, RF24_PA_MAX, \
                RF24_250KBPS, RF24_CRC_DISABLED, RF24_CRC_8, RF24_CRC_16
        LIB_RF24 = 'TRMh20'
except ImportError:
    pass

try:
    if not LIB_RF24:
        from circuitpython_nrf24l01.rf24 import RF24 as CircuitpythonRF24
        LIB_RF42 = 'Circuitpython'
except ImportError:
    pass

if LIB_RF24 == 'Circuitpython':
    RF24_PA_MIN = -18
    RF24_PA_LOW = -12
    RF24_PA_HIGH = -6
    RF24_PA_MAX = 0

    RF24_CRC_DISABLED = 0
    RF24_CRC_16 = 1
    RF24_CRC_16 = 2

    RF24_250KBPS = 250

    class RF24(CircuitpythonRF24):
        """TMRh20 like RF24 interface provider

        :param ce: Connected CE GPIO 0 or 1
        :type ce: int
        :param cs: Connected CS GPIO
        :type cs: int
        :param speed: SPI speed
        :type speed: int
        """
        def __init__(self, ce: int, cs: int, speed: int) -> None:
            """Init"""
            spi = spidev.SpiDev()
            cs = DigitalInOut(getattr(board, f'CE{cs}'))
            ce = DigitalInOut(getattr(board, f'D{ce}'))
            super().__init__(spi, cs, ce, spi_frequency=speed)

        def begin(self) -> bool:
            """Setup NRF24 module
            :return: if success
            :rtype: True or None
            """
            self.interrupt_config(False, False, False)
            self.address_length = 5
            self.allow_ask_no_ack = False
            self.power = True
            self.listen = True
            return True

        def startListening(self) -> None:
            """listen = True"""
            self.listen = True
        def stopListening(self) -> None:
            """listen = False"""
            self.listen = False

        def setDataRate(self, rate) -> None:
            """RF data rate
            :param rate: data rate
            :type rate: int
            """
            self.data_rate = rate

        def setAutoAck(self, enable, pipe=0) -> None:
            """alias for set_auto_ack"""
            self.auto_ack = enable
            self.set_auto_ack(enable, pipe)
        def getAutoAck(self, pipe=0) -> bool:
            """alias for get_auto_ack"""
            return self.get_auto_ack(pipe)

        def setCRCLength(self, n_bytes) -> None:
            """set attr crc"""
            self.crc = n_bytes

        def setChannel(self, chan) -> None:
            """set attr channel"""
            self.channel = chan

        def setPALevel(self, level) -> None:
            """set attr pa_level"""
            self.pa_level = level

        def enableDynamicPayloads(self) -> None:
            """set attr dynamic_payload = True"""
            self.dynamic_payloads = True
        def disableDynamicPayloads(self) -> None:
            """set attr dynamic_payload = False"""
            self.dynamic_payloads = False
        def getDynamicPayloadSize(self) -> int:
            """alias any"""
            return self.any()

        def openWritingPipe(self, addr) -> None:
            """alias for open_tx_pipe"""
            self.open_tx_pipe(addr)
        def openReadingPipe(self, n_pipe, addr) -> None:
            """alias for open_rx_pipe"""
            self.open_rx_pipe(n_pipe, addr)

        def setRetries(self, delay, count) -> None:
            """alias for set_auto_retries"""
            self.set_auto_retries(delay, count)

        def available_pipe(self) -> tuple:
            """alias for (available, pipe)"""
            return (self.available(), self.pipe, )

        def powerDown(self) -> None:
            """power down radio"""
            self.power = False

if not LIB_RF24:
    raise RuntimeError('Could not load Module RF24. Make shure to install TMRh20\'s RF24 or Circuitpython rf24l01')

