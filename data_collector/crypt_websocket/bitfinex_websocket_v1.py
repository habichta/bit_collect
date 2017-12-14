import json
from .abstract_websocket import AbstractWebSocketProducer, AbstractWebSocketConsumer, WebSocketHelpers, \
    ProtocolException
from logging.config import fileConfig
import logging
import hmac
import hashlib
import time
from threading import Event
from queue import Queue
import random

from pprint import pprint

# TODO move to __init__.py
fileConfig('logging_config.ini')
logger = logging.getLogger()
logger.setLevel('DEBUG')


class BitfinexWebsocketProducer_v1(AbstractWebSocketProducer):
    """
    Implement Bitfinex Protocol V1, Producer side
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    #########################
    # Exchange Protocol
    #########################

    def bitfinex_send_protocol(self, **kwargs):
        payload = json.dumps(kwargs)

        return payload

    def bitfinex_subscribe(self, **r_args):
        request = {'event': 'subscribe'}
        request.update(r_args)
        identifier = tuple(r_args.values())  # For unsubscribing
        self.send(self.bitfinex_send_protocol, **request)

        return identifier

    def bitfinex_unsubscribe(self, **r_args):
        request = {'event': 'unsubscribe'}
        request.update(r_args)
        self.send(self.bitfinex_send_protocol, **request)

    def bitfinex_ping(self, **r_args):
        cid = random.randint(0, 2 ** 10)
        request = {'event': 'ping', 'cid': cid}
        self.send(self.bitfinex_send_protocol, **request)
        return time.time(), cid


class BitfinexWebsocketConsumer_v1(AbstractWebSocketConsumer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._ws = BitfinexWebsocketProducer_v1(pc_queue=self.pc_queue, **kwargs)

        ##################################
        # BitFinex Protocol
        ##################################
        self.pl_type_switch = {'event': self._handle_event, 'data': self._handle_data}
        self.pl_event_switch = {'info': self._handle_info_event,
                                'error': self._handle_error_event,
                                'subscribed': self._handle_subscribed_event,
                                'unsubscribed': self._handle_unsubscribed_event,
                                'pong': self._handle_pong_event}
        self.pl_info_switch = {20051: self._evt_stop_handler, 20060: self._evt_resyc_start_handler,
                               20061: self._evt_resync_stop_handler}
        self.info_codes = {'20051': 'EVT_STOP', '20060': 'EVT_RESYNC_START', '20061': 'EVT_RESYNC_STOP'}
        # Stop / Restart Websocket Server(please try to reconnect),
        # Refreshing data from the Trading Engine. Pause any activity (wait for 20061),
        # Done Refreshing data from the Trading Engine. Resume normal activity. Advised to
        # unsubscribe / subscribe again all channels.
        self.error_codes = {10000: 'ERR_UNK',
                            10001: 'ERR_GENERIC',
                            10008: 'ERR_CONCURRENCY',
                            10020: 'ERR_PARAMS',
                            10050: 'ERR_CONF_FAIL',
                            10100: 'ERR_AUTH_FAIL',
                            10111: 'ERR_AUTH_PAYLOAD',
                            10112: 'ERR_AUTH_SIG',
                            10113: 'ERR_AUTH_HMAC',
                            10114: 'ERR_AUTH_NONCE',
                            10200: 'ERR_UNAUTH_FAIL',
                            10300: 'ERR_SUB_FAIL',
                            10301: 'ERR_SUB_MULTI',
                            10400: 'ERR_UNSUB_FAIL',
                            11000: 'ERR_READY',
                            }

        self.book_ch_def = {'prec': 'P0',
                            'freq': 'F0',
                            'length': '25'}

        self.bitfinex_channels = ['ticker', 'trades', 'book', 'candles']
        self.candle_timeframes = ['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M']
        self.ev_all_subscribed_fields = ['events', 'channel', 'chanId', 'pair']
        self.ev_add_orderbook_subscribed_fields = ['prec', 'freq', 'len']
        self.ev_add_raworderbook_subscribed_fields = ['prec', 'len']
        self.identifier_elements = ['channel', 'symbol', 'prec', 'freq', 'len']

    ########################################################
    # Consumer Properties
    ########################################################
    @property
    def ws(self):
        return self._ws

    ##################################
    # Open Channels
    ##################################
    ##################################
    # Subscribing/Unsubscribing
    ##################################

    def subscribe_to_trades(self, symbol):

        if symbol.startswith(('t', 'f')):
            return self.ws.bitfinex_subscribe(channel='trades', symbol=symbol)
        else:
            raise ProtocolException(msg='Symbol needs "t","f" prefix:' + symbol)

    def subscribe_to_ticker(self, symbol):

        if symbol.startswith(('t', 'f')):
            return self.ws.bitfinex_subscribe(channel='ticker', symbol=symbol)
        else:
            raise ProtocolException(msg='Symbol needs "t","f" prefix:' + symbol)

    def subscribe_to_book(self, symbol, prec=None, freq=None, length=None):

        prec = self.book_ch_def['prec'] if prec is None else str(prec)
        freq = self.book_ch_def['freq'] if freq is None else str(freq)
        length = self.book_ch_def['length'] if length is None else str(length)

        if symbol.startswith(('t', 'f')):
            return self.ws.bitfinex_subscribe(channel='book', symbol=symbol, prec=prec, freq=freq, length=length)
        else:
            raise ProtocolException(msg='Symbol needs "t","f" prefix:' + symbol)

    def subscribe_to_candles(self, channel, time_frame, symbol):

        if symbol.startswith(('t', 'f')) and time_frame in self.candle_timeframes:
            key = ':'.join([channel, time_frame, symbol])
            return self.ws.bitfinex_subscribe(channel='candles', key=key)
        else:
            raise ProtocolException(msg='Illegal Symbol or Timeframe for Candles')

    @AbstractWebSocketConsumer._is_subscribed
    def unsubscribe(self, identifier):
        self.state_machine[identifier]['_subscribed'].clear()
        channel_id = self.state_machine[identifier]['_chanId']
        self.ws.bitfinex_unsubscribe(chanId=int(channel_id))

    def unsubscribe_all(self):
        for k in self.state_machine.keys():
            if '_subscribed' in k.keys():
                self.unsubscribe(k)

    def ping(self):
        if 'pong' in self.state_machine.keys():
            del (self.state_machine['pong'])
        return self.ws.bitfinex_ping()

    def config(self):  # TODO
        pass

    ########################################################
    # Internal Functions
    ########################################################



    ##################################
    # Handle Producer-Consumer Queue
    ##################################


    def payload_handler(self, payload, **kwargs):  # as argument to pop_and_handle

        logger.debug(payload)

        if isinstance(payload[1], AbstractWebSocketProducer.Sentinel):

            _sentinel = payload[1]

        else:

            self.state_machine['hb'] = payload[0]  # Update global Heartbeat with timestamp

            if isinstance(payload[1], dict):  # events are dictionaries
                self.pl_type_switch['event'](payload=payload, **kwargs)
            else:  # data is a list
                self.pl_type_switch['data'](payload=payload, **kwargs)

    def _handle_event(self, payload, **kwargs):

        event_type = payload[1]['event']
        try:
            self.pl_event_switch[event_type](payload=payload, **kwargs)
        except KeyError as e:
            logger.error(str(e) + ', got: ' + str(event_type))
        except Exception as e:
            logger.error(str(e) + ', got: ' + str(event_type))

    def _handle_data(self, **kwargs):
        pass

    ##################################
    # Consumer Callbacks
    ##################################
    ##################################
    # Bitfinex Events
    ##################################

    def _handle_info_event(self, payload, **kwargs):

        ts, msg = payload[0], payload[1]  # receive timestamp, Json message

        if 'version' in msg:
            WebSocketHelpers.r_add(self.state_machine, ['version', '_v', msg['version']])
            WebSocketHelpers.r_add(self.state_machine, ['version', '_ts', ts])


        elif 'code' in msg:
            info_code = msg['code']
            if info_code in self.info_codes:
                try:
                    info_message = str(msg['msg'])
                except KeyError:
                    info_message = None

                WebSocketHelpers.r_add(self.state_machine, ['info', info_code, '_ts', ts])
                WebSocketHelpers.r_add(self.state_machine, ['info', info_code, '_msg', info_message])
                logger.info(str(ts) + ': ' + str(info_code) + ': ' + info_message)

                logger.info(str(ts) + ': ' + info_code + ': ' + info_message)

                self.pl_info_switch[info_code]()


            else:
                logger.error(str(ts) + ': Unknown info code: ', str(info_code))
        else:
            logger.error(str(ts) + ': Unknown info message: ' + str(msg))

    def _handle_error_event(self, payload, **kwargs):
        ts, msg = payload[0], payload[1]
        logger.error(
            str(ts) + ': Web Socket returned error: Code:' + str(msg['code']) + ', Message: ' + str(msg['msg']))
        # TODO Error handling

    def _handle_subscribed_event(self, payload, **kwargs):

        ts, msg = payload[0], payload[1]  # Json message
        chanId = msg['chanId']

        for k, v in msg.items():
            WebSocketHelpers.r_add(self.state_machine, [chanId, '_' + str(k), v])
        WebSocketHelpers.r_add(self.state_machine, [chanId, '_ts', ts])  # Subscrition Time stamp
        WebSocketHelpers.r_add(self.state_machine, [chanId, '_hb', ts])  # Time stamp last message (Channel Heartbeat)

        # Create identifier that maps to channel id (for unsubscription)

        identifier = []
        for e in self.identifier_elements:
            if e in msg.keys():
                identifier += [msg[e]]
        ident_t = tuple(identifier)
        WebSocketHelpers.r_add(self.state_machine, [ident_t, '_chanId', chanId])
        WebSocketHelpers.r_add(self.state_machine, [chanId, '_identifier', ident_t])

        # Data Queue
        data_queue = Queue()
        WebSocketHelpers.r_add_queue(self.state_machine, [chanId, data_queue])
        WebSocketHelpers.r_add_queue(self.state_machine, [ident_t, data_queue])

        ready_event = Event()
        ready_event.set()
        WebSocketHelpers.r_add(self.state_machine, [ident_t, '_subscribed', ready_event])

    def _handle_unsubscribed_event(self, payload, **kwargs):
        ts, msg = payload[0], payload[1]  # Json message
        chanId = msg['chanId']
        identifier = self.state_machine[chanId]['_identifier']
        del (self.state_machine[chanId])
        del (self.state_machine[identifier])

    def _handle_pong_event(self, payload, **kwargs):
        ts, msg = payload[0], payload[1]
        WebSocketHelpers.r_add(self.state_machine, ['pong', msg['cid'], '_ts', ts])
        WebSocketHelpers.r_add(self.state_machine, ['pong', msg['cid'], '_recv_ts', msg['ts']])

    ##################################
    # Bitfinex InfoCode Events
    ##################################

    def _evt_stop_handler(self):
        self.reconnect()

    def _evt_resyc_start_handler(self):
        self.pause()

    def _evt_resync_stop_handler(self):
        self.reconnect()
        self.unpause()

    ##################################
    # Bitfinex Data
    ##################################

    # TODO Data handling, Heartbeat, snapshots, updates

    ##################################
    # Authentication/Security
    ##################################

    def _create_authentication_payload(self, api_secret, api_key):

        nonce = int(time.time() * 1000000)
        auth_payload = 'AUTH{}'.format(nonce)
        signature = hmac.new(
            api_secret.encode(),
            msg=auth_payload.encode(),
            digestmod=hashlib.sha384
        ).hexdigest()

        auth_payload = {
            'apiKey': api_key,
            'event': 'auth',
            'authPayload': auth_payload,
            'authNonce': nonce,
            'authSig': signature
        }

        return auth_payload
