#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Hoymiles micro-inverters python shared code
"""

import struct
import time
import re
from datetime import datetime
import json
from typing import Generator
import crcmod
from .rf24 import *
from .decoders import *

f_crc_m = crcmod.predefined.mkPredefinedCrcFun('modbus')
f_crc8 = crcmod.mkCrcFun(0x101, initCrc=0, xorOut=0)

HOYMILES_TRANSACTION_LOGGING=False
HOYMILES_DEBUG_LOGGING=False

def ser_to_hm_addr(inverter_ser: str) -> bytes:
    """
    Calculate the 4 bytes that the HM devices use in their internal messages to
    address each other.

    :param str inverter_ser: inverter serial
    :return: inverter address
    :rtype: bytes
    """
    bcd = int(str(inverter_ser)[-8:], base=16)
    return struct.pack('>L', bcd)

def ser_to_esb_addr(inverter_ser: str) -> bytes:
    """
    Convert a Hoymiles inverter/DTU serial number into its
    corresponding NRF24 'enhanced shockburst' address byte sequence (5 bytes).

    The NRF library expects these in LSB to MSB order, even though the transceiver
    itself will then output them in MSB-to-LSB order over the air.

    The inverters use a BCD representation of the last 8
    digits of their serial number, in reverse byte order,
    followed by \x01.

    :param str inverter_ser: inverter serial
    :return: ESB inverter address
    :rtype: bytes
    """
    air_order = ser_to_hm_addr(inverter_ser)[::-1] + b'\x01'
    return air_order[::-1]

def print_addr(inverter_ser: bytes) -> None:
    """
    Debug print addresses

    :param str inverter_ser: inverter serial
    """
    print(f"ser# {inverter_ser} ", end='')
    print(f" -> HM  {' '.join([f'{byte:02x}' for byte in ser_to_hm_addr(inverter_ser)])}", end='')
    print(f" -> ESB {' '.join([f'{byte:02x}' for byte in ser_to_esb_addr(inverter_ser)])}")

class ResponseDecoderFactory:
    """
    Prepare payload decoder

    :param bytes response: ESB response frame to decode
    :param request: ESB request frame
    :type request: bytes
    :param inverter_ser: inverter serial
    :type inverter_ser: str
    :param time_rx: idatetime when payload was received
    :type time_rx: datetime
    """
    model = None
    request = None
    response = None
    time_rx = None

    def __init__(self, response: bytes, **params) -> None:
        self.response = response

        self.time_rx = params.get('time_rx', datetime.now())

        if 'request' in params:
            self.request = params['request']
        elif hasattr(response, 'request'):
            self.request = response.request

        if 'inverter_ser' in params:
            self.inverter_ser = params['inverter_ser']
            self.model = self.inverter_model

    def unpack(self, fmt: str, base: int) -> tuple:
        """
        Data unpack helper

        :param str fmt: struct format string
        :param int base: unpack base position from self.response bytes
        :return: unpacked values
        :rtype: tuple
        """
        size = struct.calcsize(fmt)
        return struct.unpack(fmt, self.response[base:base+size])

    @property
    def inverter_model(self) -> str:
        """
        Find decoder for inverter model

        :return: suitable decoder model string
        :rtype: str
        :raises ValueError: on invalid inverter serial
        :raises NotImplementedError: if inverter model can not be determined
        """
        if not self.inverter_ser:
            raise ValueError('Inverter serial while decoding response')

        ser_db = [
                ('Hm300', r'^1121........'),
                ('Hm600', r'^1141........'),
                ('Hm1200', r'^1161........'),
                ]
        ser_str = str(self.inverter_ser)

        model = None
        for s_model, r_match in ser_db:
            if re.match(r_match, ser_str):
                model = s_model
                break

        if len(model):
            return model
        raise NotImplementedError('Model lookup failed for serial {ser_str}')

    @property
    def request_command(self) -> str:
        """
        Return requested command identifier byte

        :return: hexlified command byte string
        :rtype: str
        """
        r_code = self.request[10]
        return f'{r_code:02x}'

class ResponseDecoder(ResponseDecoderFactory):
    """
    Base response

    :param bytes response: ESB frame response
    """
    def __init__(self, response: bytes, **params) -> None:
        """Initialize ResponseDecoder"""
        ResponseDecoderFactory.__init__(self, response, **params)

    def decode(self) -> Response:
        """
        Decode Payload

        :return: payload decoder instance
        :rtype: object
        """
        model = self.inverter_model
        command = self.request_command

        model_decoders = __import__('hoymiles.decoders')
        if hasattr(model_decoders, f'{model}Decode{command.upper()}'):
            device = getattr(model_decoders, f'{model}Decode{command.upper()}')
        else:
            if HOYMILES_DEBUG_LOGGING:
                device = getattr(model_decoders, 'DebugDecodeAny')

        return device(self.response,
                time_rx=self.time_rx,
                inverter_ser=self.inverter_ser
                )

class InverterPacketFragment:
    """ESB Frame"""
    def __init__(self, time_rx: datetime = None, payload: bytes = None, ch_rx: int = None, ch_tx: int = None, **params) -> None:
        """
        Callback: get's invoked whenever a Nordic ESB packet has been received.

        :param time_rx: datetime when frame was received
        :type time_rx: datetime
        :param payload: payload bytes
        :type payload: bytes
        :param ch_rx: channel where packet was received
        :type ch_rx: int
        :param ch_tx: channel where request was sent
        :type ch_tx: int

        :raises BufferError: when data gets lost on SPI bus
        """

        if not time_rx:
            time_rx = datetime.now()
        self.time_rx = time_rx

        self.frame = payload

        # check crc8
        if f_crc8(payload[:-1]) != payload[-1]:
            raise BufferError('Frame kaputt')

        self.ch_rx = ch_rx
        self.ch_tx = ch_tx

    @property
    def main_cmd(self) -> int:
        """Transaction counter"""
        return self.frame[0]

    @property
    def src(self) -> int:
        """
        Sender adddress

        :return: sender address
        :rtype: int
        """
        src = struct.unpack('>L', self.frame[1:5])
        return src[0]
    @property
    def dst(self) -> int:
        """
        Receiver adddress

        :return: receiver address
        :rtype: int
        """
        dst = struct.unpack('>L', self.frame[5:8])
        return dst[0]
    @property
    def seq(self) -> int:
        """
        Framne sequence number

        :return: sequence number
        :rtype: int
        """
        result = struct.unpack('>B', self.frame[9:10])
        return result[0]
    @property
    def data(self) -> bytes:
        """
        Data without protocol framing

        :return: payload chunk
        :rtype: bytes
        """
        return self.frame[10:-1]

    def __str__(self) -> str:
        """
        Represent received ESB frame

        :return: log line received frame
        :rtype: str
        """
        c_datetime = self.time_rx.strftime("%Y-%m-%d %H:%M:%S.%f")
        size = len(self.frame)
        channel = f' channel {self.ch_rx}' if self.ch_rx else ''
        raw = " ".join([f"{b:02x}" for b in self.frame])
        return f"{c_datetime} Received {size} bytes{channel}: {raw}"

class HoymilesNRF:
    """Hoymiles NRF24 Interface"""
    tx_channel_id = 0
    tx_channel_list = [40]
    rx_channel_id = 0
    rx_channel_list = [3,23,40,61,75]
    rx_channel_ack = False
    rx_error = 0
    txpower = 'max'

    def __init__(self, **radio_config) -> None:
        """
        Claim radio device

        :param NRF24 device: instance of NRF24
        """
        radio = RF24(
                radio_config.get('ce_pin', 22),
                radio_config.get('cs_pin', 0),
                radio_config.get('spispeed', 1000000))

        if not radio.begin():
            raise RuntimeError('Can\'t open radio')

        self.txpower = radio_config.get('txpower', 'max')

        self.radio = radio

    def transmit(self, packet: bytes, txpower: int = None) -> bool:
        """
        Transmit Packet

        :param bytes packet: buffer to send
        :return: if ACK received of ACK disabled
        :rtype: bool
        """

        if not txpower:
            txpower = self.txpower

        inv_esb_addr = b'\01' + packet[1:5]
        dtu_esb_addr = b'\01' + packet[5:9]

        self.radio.stopListening()  # put radio in TX mode
        self.radio.setDataRate(RF24_250KBPS)
        self.radio.openReadingPipe(1,dtu_esb_addr)
        self.radio.openWritingPipe(inv_esb_addr)
        self.radio.setChannel(self.tx_channel)
        self.radio.setAutoAck(True)
        self.radio.setRetries(3, 15)
        self.radio.setCRCLength(RF24_CRC_16)
        self.radio.enableDynamicPayloads()

        if txpower == 'min':
            self.radio.setPALevel(RF24_PA_MIN)
        elif txpower == 'low':
            self.radio.setPALevel(RF24_PA_LOW)
        elif txpower == 'high':
            self.radio.setPALevel(RF24_PA_HIGH)
        else:
            self.radio.setPALevel(RF24_PA_MAX)

        if hasattr(self.radio, 'send'):
            res = self.radio.send(packet)
        else:
            res = self.radio.write(packet)
        return res

    def receive(self, timeout: int = None) -> Generator[bytes, None, None]:
        """
        Receive Packets

        :param timeout: receive timeout in nanoseconds (default: 12e8)
        :type timeout: int
        :yields: fragment
        """

        if not timeout:
            timeout=12e8

        self.radio.setChannel(self.rx_channel)
        self.radio.setAutoAck(False)
        self.radio.setRetries(0, 0)
        self.radio.enableDynamicPayloads()
        self.radio.setCRCLength(RF24_CRC_16)
        self.radio.startListening()

        fragments = []

        # Receive: Loop
        t_end = time.monotonic_ns()+timeout
        while time.monotonic_ns() < t_end:

            has_payload, pipe_number = self.radio.available_pipe()
            if has_payload:

                # Data in nRF24 buffer, read it
                self.rx_error = 0
                self.rx_channel_ack = True
                t_end = time.monotonic_ns()+5e8

                size = self.radio.getDynamicPayloadSize()
                payload = self.radio.read(size)
                fragment = InverterPacketFragment(
                        payload=payload,
                        ch_rx=self.rx_channel, ch_tx=self.tx_channel,
                        time_rx=datetime.now()
                        )

                yield fragment

            else:

                # No data in nRF rx buffer, search and wait
                # Channel lock in (not currently used)
                self.rx_error = self.rx_error + 1
                if self.rx_error > 1:
                    self.rx_channel_ack = False
                # Channel hopping
                if self.next_rx_channel():
                    self.radio.stopListening()
                    self.radio.setChannel(self.rx_channel)
                    self.radio.startListening()

            time.sleep(0.005)

    def next_rx_channel(self) -> bool:
        """
        Select next channel from hop list
        - if hopping enabled
        - if channel has no ack

        :return: if new channel selected
        :rtype: bool
        """
        if not self.rx_channel_ack:
            self.rx_channel_id = self.rx_channel_id + 1
            if self.rx_channel_id >= len(self.rx_channel_list):
                self.rx_channel_id = 0
            return True
        return False

    @property
    def tx_channel(self) -> int:
        """
        Get current tx channel

        :return: tx_channel
        :rtype: int
        """
        return self.tx_channel_list[self.tx_channel_id]

    @property
    def rx_channel(self) -> int:
        """
        Get current rx channel

        :return: rx_channel
        :rtype: int
        """
        return self.rx_channel_list[self.rx_channel_id]

    def __del__(self):
        self.radio.powerDown()

def frame_payload(payload: bytes) -> bytes:
    """
    Prepare payload for transmission, append Modbus CRC16

    :param bytes payload: payload to be prepared
    :return: payload + crc
    :rtype: bytes
    """
    payload_crc = f_crc_m(payload)
    payload = payload + struct.pack('>H', payload_crc)

    return payload

def compose_esb_fragment(fragment: bytes, maincmd: bytes = b'\x15', subcmd: bytes = b'\x80\x0b',
        src: int = 99999999, dst: int = 1, **params) -> bytes:
    """
    Build standart ESB request fragment

    :param bytes fragment: up to 16 bytes payload chunk
    :param seq: frame sequence byte
    :type seq: bytes
    :param src: dtu address
    :type src: int
    :param dst: inverter address
    :type dst: int
    :return: esb frame fragment
    :rtype: bytes
    :raises ValueError: if fragment size larger 16 byte
    """
    if len(fragment) > 17:
        raise ValueError(f'ESB fragment exeeds mtu: Fragment size {len(fragment)} bytes')

    packet = b'' + maincmd
    packet = packet + ser_to_hm_addr(dst)
    packet = packet + ser_to_hm_addr(src)
    packet = packet + subcmd

    packet = packet + fragment

    crc8 = f_crc8(packet)
    packet = packet + struct.pack('B', crc8)

    return packet

def compose_esb_packet(packet: bytes, mtu: int = 17, **params) -> Generator[bytes, None, None]:
    """
    Build ESB packet, chunk packet

    :param bytes packet: payload data
    :param mtu: maximum transmission unit per frame (default: 17)
    :type mtu: int
    :yields: fragment
    """
    for i in range(0, len(packet), mtu):
        fragment = compose_esb_fragment(packet[i:i+mtu], **params)
        yield fragment

class ESBFrame:
    l_addr = 4
    preamble = b'\x15'
    target = b'\x00\x00\x00\x00'
    source = b'\x00\x00\x00\x00'
    payload = b''

    @staticmethod
    def frombytes(data: bytes, **params):
        l_addr = params.get('address_length', ESBFrame.l_addr)
        o_target = 1 + l_addr
        o_data = o_target + l_addr

        return ESBFrame(
                preamble=data[:1],
                source=data[o_target:o_data],
                target=data[1:o_target],
                payload=data[o_data:-1],
                **params)

    @staticmethod
    def fromhex(data: str, **params):
        return ESBFrame.frombytes(bytes.fromhex(data))

    def __init__(self, **params) -> None:
        self.payload = params.get('payload', b'')
        self.l_addr = params.get('address_length', ESBFrame.l_addr)
        self.set_source(params['source'])
        self.set_target(params['target'])

    def set_preamble(self, preamble: bytes) -> None:
        if len(source) != 1:
            raise ValueError(f'Set invalid preamble legth {len(preamble)}, required 1')
        self.preamble = preamble

    def set_source(self, addr: int) -> None:
        if len(addr) != self.l_addr:
            raise ValueError(f'Set invalid source address legth {len(addr)}, required {self.l_addr}')
        self.source = addr

    def set_target(self, addr: int) -> None:
        if len(addr) != self.l_addr:
            raise ValueError(f'Set invalid target address legth {len(addr)}, required {self.l_addr}')
        self.target = addr

    @property
    def packet(self) -> bytes:
        packet = self.preamble
        packet = packet + self.target
        packet = packet + self.source
        packet = packet + self.payload
        return packet

    @property
    def crc(self) -> bytes:
        crc8 = f_crc8(self.packet)
        return struct.pack('B', crc8)

    def __bytes__(self) -> bytes:
        return self.packet + self.crc

    def __repr__(self) -> str:
        return hexify_payload(self.__bytes__())

class RequestFactory:
    _maincmd = b'\x15'
    _source = b'\x00\x00\x00\x00'
    _target = b'\x00\x00\x00\x00'
    _subcmd = b''
    _payload = b''
    _mtu = 16

    def __init__(self, payload: bytes, **params) -> None:
        self._payload = payload

        if 'dtu_ser' in params:
            self.source(params['dtu_ser'])

        if 'inverter_ser' in params:
            self.target(params['inverter_ser'])

        if 'maincmd' in params:
            self.maincmd(params['maincmd'])

    def source(self, source: int) -> None:
        self._source = ser_to_hm_addr(source)

    def target(self, target: int) -> None:
        self._target = ser_to_hm_addr(target)

    def maincmd(self, maincmd: bytes) -> None:
        self._maincmd = maincmd

    def subcmd(self, subcmd: bytes) -> None:
        self._subcmd = subcmd

    @property
    def fragment(self, num: int) -> None:
        return

    @property
    def crc(self) -> bytes:
        crc = f_crc_m(self._payload)
        return struct.pack('>H', crc)

    def __iter__(self) -> Generator[ESBFrame, None, None]:
        n_frame = 0x00
        payload = self._payload + self.crc
        l_payload = len(payload)
        for i_base in range(0, l_payload, self._mtu):
            n_frame = n_frame + 0x01
            if i_base + self._mtu >= l_payload:
                n_frame = n_frame + 0x80
            subcmd = struct.pack('>B', n_frame)
            yield ESBFrame(
                    preamble=self._maincmd,
                    source=self._source,
                    target=self._target,
                    payload=subcmd + payload[i_base:i_base+self._mtu])

def compose_set_time_payload(timestamp: int = None) -> bytes:
    """
    Build set time request packet

    :param timestamp: time to set (default: int(time.time()) )
    :type timestamp: int
    :return: payload
    :rtype: bytes
    """
    if not timestamp:
        timestamp = int(time.time())

    payload = b'\x0b\x00'
    payload = payload + struct.pack('>L', timestamp)  # big-endian: msb at low address
    payload = payload + b'\x00\x00\x00\x05\x00\x00\x00\x00'

    return frame_payload(payload)

class InverterTransaction:
    """
    Inverter transaction buffer, implements transport-layer functions while
    communicating with Hoymiles inverters
    """
    tx_queue = []
    scratch = []
    inverter_ser = None
    inverter_addr = None
    dtu_ser = None
    req_type = None
    time_rx = None

    radio = None
    txpower = None

    def __init__(self,
            request_time: datetime = None,
            inverter_ser: str = None,
            dtu_ser: str = None,
            radio: HoymilesNRF = None,
            **params) -> None:
        """
        :param request: Transmit ESB packet
        :type request: bytes
        :param request_time: datetime of transmission
        :type request_time: datetime
        :param inverter_ser: inverter serial
        :type inverter_ser: str
        :param dtu_ser: DTU serial
        :type dtu_ser: str
        :param radio: HoymilesNRF instance to use
        :type radio: HoymilesNRF or None
        """

        if radio:
            self.radio = radio

            if 'txpower' in params:
                self.txpower = params['txpower']

        if not request_time:
            request_time=datetime.now()

        self.scratch = []
        if 'scratch' in params:
            self.scratch = params['scratch']

        self.inverter_ser = inverter_ser
        if inverter_ser:
            self.inverter_addr = ser_to_hm_addr(inverter_ser)

        self.dtu_ser = dtu_ser
        if dtu_ser:
            self.dtu_addr = ser_to_hm_addr(dtu_ser)

        self.request = None
        if 'request' in params:
            self.request = params['request']
            self.queue_tx(self.request)
            self.inverter_addr, self.dtu_addr, seq, self.req_type = struct.unpack('>LLBB', params['request'][1:11])
        self.request_time = request_time

    def rxtx(self) -> bool:
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

        if HOYMILES_TRANSACTION_LOGGING:
            c_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            print(f'{c_datetime} Transmit {len(packet)} | {hexify_payload(packet)}')

        self.radio.transmit(packet, txpower=self.txpower)

        wait = False
        try:
            for response in self.radio.receive():
                if HOYMILES_TRANSACTION_LOGGING:
                    print(response)

                self.frame_append(response)
                wait = True
        except TimeoutError:
            pass

        return wait

    def frame_append(self, frame: bytes) -> bytes:
        """
        Append received raw frame to local scratch buffer

        :param bytes frame: Received ESB frame
        :return None
        """
        self.scratch.append(frame)

    def queue_tx(self, frame: bytes) -> bool:
        """
        Enqueue packet for transmission if radio is available

        :param bytes frame: ESB frame for transmit
        :return: if radio is available and frame scheduled
        :rtype: bool
        """
        if not self.radio:
            return False

        self.tx_queue.append(frame)

        return True

    def get_payload(self, src: bytes = None) -> bytes:
        """
        Reconstruct Hoymiles payload from scratch buffer

        :param src: filter frames by inverter hm_address (default self.inverter_address)
        :type src: bytes
        :return: payload
        :rtype: bytes
        :raises BufferError: if one or more frames are missing
        :raises ValueError: if assambled payload fails CRC check
        """

        if not src:
            src = self.inverter_addr

        # Collect all frames from source_address src
        frames = [frame for frame in self.scratch if frame.src == src]

        tr_len = 0
        # Find end frame and extract message frame count
        try:
            end_frame = next(frame for frame in frames if frame.seq > 0x80)
            self.time_rx = end_frame.time_rx
            tr_len = end_frame.seq - 0x80
        except StopIteration:
            seq_last = max(frames, key=lambda frame:frame.seq).seq
            self.__retransmit_frame(seq_last + 1)
            raise BufferError(f'Missing packet: Last packet {len(self.scratch)}')

        # Rebuild payload from unordered frames
        payload = b''
        for frame_id in range(1, tr_len):
            try:
                data_frame = next(item for item in frames if item.seq == frame_id)
                payload = payload + data_frame.data
            except StopIteration:
                self.__retransmit_frame(frame_id)
                raise BufferError(f'Frame {frame_id} missing: Request Retransmit')

        payload = payload + end_frame.data

        # check crc
        pcrc = struct.unpack('>H', payload[-2:])[0]
        if f_crc_m(payload[:-2]) != pcrc:
            raise ValueError('Payload failed CRC check.')

        return (end_frame.main_cmd, payload,)

    def __retransmit_frame(self, frame_id: int) -> bytes:
        """
        Build and queue retransmit request

        :param int frame_id: frame id to re-schedule
        :return: if successful scheduled
        :rtype: bool
        """

        if not self.radio:
            return

        packet = compose_esb_fragment(b'',
                subcmd=int(0x80 + frame_id).to_bytes(1, 'big'),
                src=self.dtu_ser,
                dst=self.inverter_ser)

        return self.queue_tx(packet)

    def __str__(self) -> str:
        """
        Represent transmit payload

        :return: log line of payload for transmission
        :rtype: str
        """
        c_datetime = self.request_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        size = len(self.request)
        return f'{c_datetime} Transmit | {hexify_payload(self.request)}'

def hexify_payload(byte_var: bytes) -> str:
    """
    Represent bytes

    :param bytes byte_var: bytes to be hexlified
    :return: two-byte while-space padded byte representation
    :rtype: str
    """
    return ' '.join([f"{b:02x}" for b in byte_var])
