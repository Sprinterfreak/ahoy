#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import codecs
import re
import time
from datetime import datetime
import hoymiles

logdata = """
2022-05-01 12:29:02.139673 Transmit 368223: channel=40 len=27 ack=False | 15 72 22 01 43 78 56 34 12 80 0b 00 62 6e 60 ee 00 00 00 05 00 00 00 00 7e 58 25
2022-05-01 12:29:02.184796 Received 27 bytes on channel 3 after tx 6912328ns: 95 72 22 01 43 72 22 01 43 01 00 01 01 4e 00 9d 02 0a 01 50 00 9d 02 10 00 00 91
2022-05-01 12:29:02.184796 Decoder src=72220143, dst=72220143, cmd=1,   u1=33.4V, i1=1.57A, p1=52.2W,  u2=33.6V, i2=1.57A, p2=52.8W,  uk1=1, uk2=0
2022-05-01 12:29:02.226251 Received 27 bytes on channel 75 after tx 48355619ns: 95 72 22 01 43 72 22 01 43 02 88 1f 00 00 7f 08 00 94 00 97 08 e2 13 89 03 eb ec
2022-05-01 12:29:02.226251 Decoder src=72220143, dst=72220143, cmd=2,   ac_u1=227.4V, ac_f=50.01Hz, ac_p1=100.3W, uk1=34847, uk2=0, uk3=32520, uk4=148, uk5=151
2022-05-01 12:29:02.273766 Received 23 bytes on channel 75 after tx 95876606ns: 95 72 22 01 43 72 22 01 43 83 00 01 00 2c 03 e8 00 d8 00 06 0c 35 37
2022-05-01 12:29:02.273766 Decoder src=72220143, dst=72220143, cmd=131,   ac_i1=0.44A, t=21.60C,  uk1=1, uk3=1000, uk5=6, uk6=3125
"""

hoymiles.HOYMILES_DEBUG_LOGGING=True

def payload_from_log(line):
    values = re.match(r'(?P<datetime>\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+) Received.*: (?P<data>[0-9a-z ]+)$', line)
    if values:
        payload=values.group('data')
        return hoymiles.InverterPacketFragment(
            time_rx=datetime.strptime(values.group('datetime'), '%Y-%m-%d %H:%M:%S.%f'),
            payload=bytes.fromhex(payload)
            )

with open('/var/log/nahoy.log', 'r') as fh:
    for line in fh:
        kind = re.match(r'\d{4}-\d{2}-\d{2} \d\d:\d\d:\d\d.\d+ (?P<type>Transmit|Received)', line)
        if kind:
            if kind.group('type') == 'Transmit':
                u, data = line.split('|')
                rx_buffer = hoymiles.InverterTransaction(
                        request=bytes.fromhex(data),
                        inverter_ser=114172220143)

            elif kind.group('type') == 'Received':
                try:
                    payload = payload_from_log(line)
                    print(payload)
                except BufferError as err:
                    print(f'Debug: {err}')
                    payload = None
                    pass
                if payload:
                    rx_buffer.frame_append(payload)
                    try:
                        packet = rx_buffer.get_payload()
                    except ValueError as err:
                        print(f'Debug: {err}')
                        packet = None
                        pass
                    except BufferError:
                        packet = None
                        pass

                    if packet:
                        response = hoymiles.ResponseDecoder(packet, inverter_ser=114172220143, request=rx_buffer.request)
                        res = response.decode()
                        dt = rx_buffer.time_rx.strftime("%Y-%m-%d %H:%M:%S.%f")

                        if isinstance(res, hoymiles.decoders.Hm600Decode0B):
                            print(f'{dt} Decoded: {len(packet)}', end='')
                            string_id = 0
                            for string in res.strings:
                                string_id = string_id + 1
                                print(f' string{string_id}=', end='')
                                print(f' {string["voltage"]}VDC', end='')
                                print(f' {string["current"]}A', end='')
                                print(f' {string["power"]}W', end='')
                                print(f' {string["energy_total"]}Wh', end='')
                                print(f' {string["energy_daily"]}Wh/day', end='')

                            phase_id = 0
                            for phase in res.phases:
                                phase_id = phase_id + 1
                                print(f' phase{phase_id}=', end='')
                                print(f' {phase["voltage"]}VAC', end='')
                                print(f' {phase["current"]}A', end='')
                                print(f' {phase["power"]}W', end='')
                            print(f' inverter={response.inverter_ser}', end='')
                            print(f' {res.frequency}Hz', end='')
                            print(f' {res.temperature}°C', end='')
                            print(f' {res.alarm_count} Alarms', end='')
                            print()
                        if isinstance(res, hoymiles.decoders.Hm600Decode11):
                            print(packet)

        print('', end='', flush=True)
