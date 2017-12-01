import json
from .abstract_websocket import AbstractWebSocketProducer, AbstractWebSocketConsumer
from logging.config import fileConfig
from threading import Thread
import logging
import time
import queue

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

    def bitfinex_send_protocol(self, api_key=None, secret=None, auth=False, **kwargs):
        payload = json.dumps(kwargs)

        return payload

    def bitfinex_subscribe(self, channel, **r_args):
        request = {'event': 'subscribe', 'channel': channel}
        request.update(r_args)
        self.send(self.bitfinex_send_protocol, **request)

    #########################
    # Producer Callbacks
    #########################

    def on_message(self, *args):
        msg_dict, receive_ts = json.loads(args[1]), time.time()
        self.pc_queue.put((receive_ts, msg_dict))
        logger.debug(msg_dict)

    @AbstractWebSocketProducer._on_close
    def on_close(self, *args):
        # TODO Sentinel?
        print('Closed')

    @AbstractWebSocketProducer._on_open
    def on_open(self, *args):
        # TODO Authentication methods
        pass

    def on_error(self, *args):
        # TODO Sentinel?
        logger.error('Arguments: ' + str(args))


class BitfinexWebsocketConsumer_v1(AbstractWebSocketConsumer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.ws = BitfinexWebsocketProducer_v1(pc_queue=self.pc_queue, **kwargs)

        # Protocol switch
        self.pl_type_switch = {'event': self._handle_event, 'data': self._handle_data}
        self.pl_event_switch = {'info': self._handle_info_event,
                                'error': self._handle_error_event,
                                'subscribed': self._handle_subscribed_event,
                                'unsubscribed': self._handle_unsubscribed_event,
                                'pong': self._handle_pong_event}


        # Event: info,error,subscribed,unsubsribed, ping,pong

    ##################################
    # Connect/Disconnect
    ##################################
    def connect(self):
        self.ws.start()
        while not self.ws.connected:
            # Wait for WebSocket Thread to establish connection
            print('Establishing Connection to ', self.ws.uri)
            time.sleep(1)

    def disconnect(self):
        self.ws.close()
        # TODO reset state machine
        if self.ws is not None and self.ws.ident:  # Check if Producer Websocket Thread is running
            self.ws.join()

    ##################################
    # Handle Producer-Consumer Queue
    ##################################


    def _payload_handler(self, payload, **kwargs):  # as argument to pop_and_handle


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

    def _handle_data(self, **kwargs):
        pass

    ##################################
    # Consumer Callbacks
    ##################################
    ##################################
    # Bitfinex Events
    ##################################

    def _handle_info_event(self, payload, **kwargs):

        print(payload)

    def _handle_error_event(self, payload, **kwargs):
        print(payload)

    def _handle_subscribed_event(self, payload, **kwargs):
        print(payload)

    def _handle_unsubscribed_event(self, payload, **kwargs):

        print(payload)

    def _handle_pong_event(self, payload, **kwargs):
        print(payload)

    ##################################
    # Bitfinex Data
    ##################################




    ##################################
    # Open Channels
    ##################################
    ##################################
    # Subscribing/Unsubscribing
    ##################################

    def subscribe_to_trades(self, pair):
        self.ws.bitfinex_subscribe(channel='trades', pair=pair)

    def subscribe_to_ticker(self, pair):
        self.ws.bitfinex_subscribe(channel='ticker', pair=pair)
