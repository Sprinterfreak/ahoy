#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Search Hoymiles inverters serial
"""

import sys
import time
import json
import paho.mqtt.client as mqtt
from datetime import datetime
import hoymiles
from RF24 import RF24, RF24_PA_LOW, RF24_PA_MAX, RF24_250KBPS, RF24_CRC_DISABLED, RF24_CRC_8, RF24_CRC_16

hoymiles.HOYMILES_TRANSACTION_LOGGING=True
hoymiles.HOYMILES_DEBUG_LOGGING=True

class InverterTransaction(hoymiles.InverterTransaction):
    def rxtx(self, **params):
        """
        Transmit next packet from tx_queue if available
        and wait for responses

        :return: if we got contact
        :rtype: bool
        """
        if not self.radio:
            return False

        if len(self.tx_queue) == 0:
            return False

        packet = self.tx_queue.pop(0)

        packet = bytearray(packet)

        packet[1] = 0
        packet[2] = 0
        packet[3] = 0
        packet[4] = 0

        if hoymiles.HOYMILES_TRANSACTION_LOGGING:
            c_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            print(f'{c_datetime} Transmit {len(packet)} | {hoymiles.hexify_payload(packet)}')

        self.radio.transmit(packet)

        wait = False
        try:
            for response in self.radio.receive(**params):
                if hoymiles.HOYMILES_TRANSACTION_LOGGING:
                    print(response)

                self.frame_append(response)
                wait = True
        except TimeoutError:
            pass

        return wait

def main():
    global inverter_ser

    try:
        with open('lastserial', 'r') as fh:
            last_ser = int(fh.read())
    except FileNotFoundError:
        last_ser = 116160000000
        pass

    radio_config = {}
    dtu_ser = 99978563412
    radio = RF24(
            radio_config.get('ce_pin', 22),
            radio_config.get('cs_pin', 0),
            radio_config.get('spispeed', 1000000))
    if not radio.begin():
        raise RuntimeError('Can\'t open radio')

    hmradio = hoymiles.HoymilesNRF(device=radio)

    retries = 2

    for inverter_ser in range(last_ser, 4294967296):

        # Wait until the inverter produces power
        while not scan_enable:
            time.sleep(60)

        payload = hoymiles.compose_set_time_payload()

        payload_ttl = retries
        while payload_ttl > 0:
            payload_ttl = payload_ttl - 1
            com = InverterTransaction(
                    radio=hmradio,
                    dtu_ser=dtu_ser,
                    inverter_ser=inverter_ser,
                    request=next(hoymiles.compose_esb_packet(
                        payload,
                        seq=b'\x80',
                        src=dtu_ser,
                        dst=inverter_ser
                        )))
            response = None
            while com.rxtx(timeout=5e7):
                try:
                    response = com.get_payload()
                    payload_ttl = 0
                except Exception as e_all:
                    print(f'Found serial: {inverter_ser}')
                    raise KeyboardInterrupt(f'Found serial: {inverter_ser}')
                    sys.exit()

        # Handle the response data if any
        if response:
            print(f'Found serial: {inverter_ser}')
            raise KeyboardInterrupt(f'Found serial: {inverter_ser}')
            sys.exit()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("tele/tasmota_3EBF5D/SENSOR")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global scan_enable

    print(msg.topic+" "+str(msg.payload))

    if msg.topic == 'tele/tasmota_3EBF5D/SENSOR':
        try:
            jd = json.loads(msg.payload.decode('utf-8', 'ignore'))
            power = jd.get('ENERGY', {}).get('Power', 0)
        except Exception:
            power = 1
            jd = {}

        scan_enable = True if power > 0 else False

if __name__ == '__main__':
    inverter_ser = None
    scan_enable = True

    if False:
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect("192.168.23.30", 1883, 60)
        client.loop_start()

    try:
        main()
    except KeyboardInterrupt:
        with open('lastserial', 'w') as fh:
            fh.write(str(inverter_ser))

