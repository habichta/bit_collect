import websocket
from threading import Thread
from threading import Event
import time
from datetime import datetime
import logging
from logging.config import fileConfig
import abc
import threading
import collections
from queue import Queue
import queue
import json

# TODO
#Client interface how to share locals, WebsocketManager? kwargs ...
#Handle data
#HitBTC
# TODO move to __init__.py
fileConfig('logging_config.ini')
logger = logging.getLogger()



queue_name_const= '_dataQueue'



class AbstractWebSocketProducer(Thread, metaclass=abc.ABCMeta):
    def __init__(self, **kwargs):

        super(AbstractWebSocketProducer, self).__init__()
        self._ws = None
        self._uri = kwargs['uri']

        # State of WS
        self._connected = Event()
        self._paused = Event()
        self._reconnect_scheduled = Event()

        # Inter-Thread Communication, Producer-consumer queue
        self.pc_queue = kwargs['pc_queue']

        ##############################
        # WebSocket Protocol
        ##############################

        self.sentinel_codes = {'WS_CONN_CLOSED': 0x00000001, 'WS_CONN_ERROR': 0x00000002, 'WS_CONN_OPEN': 0x00000004,
                      'WS_CONN_PAUSED': 0x00000008, 'WS_CONN_UNPAUSED': 0x00000010,'WS_CONN_DISCONNECT':0x00000020}

    def __repr__(self):

        _s = super().__repr__() + ', WebSocket for ' + str(self._uri)
        return _s

    def run(self):
        self.connect()

    ##############################
    # Properties
    ##############################


    @property
    def uri(self):
        return self._uri

    @property
    def connected(self):
        return self._connected.is_set()

    @property
    def reconnect_scheduled(self):
        return self._reconnect_scheduled.is_set()

    @reconnect_scheduled.setter
    def reconnect_scheduled(self, value):
        if value:
            self._reconnect_scheduled.set()
        else:
            self._reconnect_scheduled.clear()

    @property
    def paused(self):
        return self._paused.is_set()

    @paused.setter
    def paused(self, value):
        if value:
            self._paused.set()
        else:
            self._paused.clear()

    @property
    def terminated(self):
        return (not self.connected) and (not self.reconnect_scheduled)

    @property
    def ws(self):
        return self._ws

    ##############################
    # Static Methods
    ##############################

    @staticmethod
    def _is_connected(func):
        def inner(self, *args, **kwargs):
            if self.ws and self.connected:
                return func(self, *args, **kwargs)
            else:
                logger.error('Web Socket not connected')
                return None

        return inner

    @staticmethod
    def _is_not_paused(func):
        def inner(self, *args, **kwargs):
            if self.ws and not self.paused:
                return func(self, *args, **kwargs)
            else:
                logger.error('Web Socket paused')
                return None

        return inner

    ###################################
    # Parent Callbacks/Abstract Methods
    ###################################


    def on_message(self, *args):
        return


    def on_error(self, *args):
        return


    def on_close(self, *args):
        return


    def on_open(self, *args):
        return

    def on_message_cb(self, *args):
        msg_dict, receive_ts = json.loads(args[1]), time.time()
        self.pc_queue.put((receive_ts, msg_dict))
        self.on_message(*args)

    def on_error_cb(self, *args):
        self._connected.clear()
        self.pc_queue.put((time.time(), AbstractWebSocketProducer.Sentinel(code=self.sentinel_codes['WS_CONN_ERROR'])))
        logger.error('Callback Error occurred with arguments: ' + str(args))
        self.on_error(*args)

    def on_close_cb(self, *args):
        self._connected.clear()

        if self.reconnect_scheduled:
            self.pc_queue.put((time.time(), AbstractWebSocketProducer.Sentinel(code=self.sentinel_codes['WS_CONN_CLOSED'])))
        else:
            self.pc_queue.put((time.time(), AbstractWebSocketProducer.Sentinel(code=self.sentinel_codes['WS_CONN_DISCONNECT'])))

        self.on_close(*args)

    def on_open_cb(self, *args):
        self._connected.set()
        self.pc_queue.put((time.time(), AbstractWebSocketProducer.Sentinel(code=self.sentinel_codes['WS_CONN_OPEN'])))
        self.on_open(*args)

    ##############################
    # Decorators
    ##############################

    def _connect_wrapper(func):
        """Generic Wrapper"""

        def inner(self, *args, **kwargs):
            self.start_epoch_time = time.time()
            self.start_utc_time = datetime.utcnow()
            logger.info('Start ' + repr(self) + ', Thread_ID: ' + str(threading.get_ident()))

            return func(self, *args, **kwargs)

        return inner

    def _close_wrapper(func):
        """Generic Wrapper"""

        def inner(self, *args, **kwargs):
            self.stop_epoch_time = time.time()
            self.stop_utc_time = datetime.utcnow()
            logger.info('Close ' + repr(self) + ', Thread_ID: ' + str(threading.get_ident()))
            return func(self, *args, **kwargs)

        return inner

    def _send_wrapper(func):
        """Generic Wrapper"""

        def inner(self, *args, **kwargs):
            return func(self, *args, **kwargs)

        return inner

    ##############################
    # Basic Functions
    ##############################

    @_connect_wrapper
    def connect(self, reconnect=True, time_interval=10):

        self._reconnect_scheduled.set()  # Can be changed by other Thread (thread-safe)

        while self._reconnect_scheduled.is_set():
            try:

                self._ws = websocket.WebSocketApp(self._uri, on_message=self.on_message_cb, on_open=self.on_open_cb,
                                                  on_error=self.on_error_cb,
                                                  on_close=self.on_close_cb)

                self._ws.run_forever()  # Stopped when connection closed or error


            except websocket.WebSocketException as e:
                logger.error('connect() failed, trace:' + str(e))

            finally:
                logger.info('Connection to {} stop'.format(self.uri))
                if self._reconnect_scheduled.is_set() and reconnect:
                    logger.info('Reconnect to {} scheduled...'.format(self.uri))
                    time.sleep(time_interval)
                    logger.info('Reconnect to {}'.format(self.uri))

    @_is_connected.__func__
    @_close_wrapper
    def close(self):
        try:
            self._ws.close()
        except websocket.WebSocketException as e:
            logger.error('close() failed, trace:' + str(e))

    @_is_not_paused.__func__
    @_is_connected.__func__
    @_send_wrapper
    def send(self, protocol_func, **kwargs):
        payload = protocol_func(**kwargs)
        try:
            self._ws.send(payload)
        except websocket.WebSocketException as e:
            logger.error('send() failed for' + str(kwargs) + ', trace:' + str(e))

    class Sentinel:
        def __init__(self, code, *args, **kwargs):
            self.kwargs = kwargs
            self.args = args
            self.code = code


class AbstractWebSocketConsumer(Thread, metaclass=abc.ABCMeta):
    def __init__(self, **kwargs):

        super(AbstractWebSocketConsumer, self).__init__()

        # Abstract State Machine for connection state
        self._state_reset()

        # Inter-Thread Communication
        self._queue = Queue()

    def _state_reset(self):
        self._state_machine = dict()

    ##############################
    # Properties
    ##############################
    @property
    def state_machine(self):
        return self._state_machine

    @property
    def pc_queue(self):  # Producer-consumer queue
        return self._queue

    ##############################
    # Static Methods
    ##############################

    @staticmethod
    def _is_subscribed(func):
        def inner(self, identifier, *args, **kwargs):
            try:
                if self.state_machine[identifier]['_subscribed'].is_set():
                    return func(self, identifier, *args, **kwargs)
                else:
                    return None
            except KeyError:
                return None

        return inner

    ##############################
    # Abstract Methods
    ##############################

    def run(self):

        self.connect()

        while not self.ws.terminated:
            self._pop_and_handle()
            self.execution_loop()
            time.sleep(0.1)


    def _payload_handler(self, payload, **kwargs):

        if isinstance(payload[1], AbstractWebSocketProducer.Sentinel):

            _sentinel = payload[1]

            if _sentinel.code == self.ws.sentinel_codes['WS_CONN_OPEN']:
                self.initialization()

        self.payload_handler(payload,**kwargs)

    def execution_loop(self):
        return

    @abc.abstractmethod
    def payload_handler(self,payload, **kwargs):
        return

    #@abc.abstractmethod
    def initialization(self):
        return

    @property
    @abc.abstractmethod
    def ws(self):  # set a protocol specific websocket (property)
        pass

    @abc.abstractmethod
    def unsubscribe_all(self, *args):
        return

    ##############################
    # Consumer Decorators
    ##############################
    def _connect_wrapper(func):
        """Generic Wrapper"""

        def inner(self, *args, **kwargs):
            self._state_reset()
            return func(self, *args, **kwargs)

        return inner

    def _disconnect_wrapper(func):
        """Generic Wrapper"""

        def inner(self, *args, **kwargs):
            self._state_reset()

            self.ws.reconnect_scheduled = False
            return func(self, *args, **kwargs)

        return inner

    def _reconnect_wrapper(func):
        """Generic Wrapper"""

        def inner(self, *args, **kwargs):
            self._state_reset()
            self.ws.reconnect_scheduled = True
            return func(self, *args, **kwargs)

        return inner

    ##############################
    # Consumer Methods
    ##############################

    def _pop_and_handle(self, block=False, timeout=None, **kwargs):

        try:
            payload = self.pc_queue.get(block=block, timeout=timeout)
            self._payload_handler(payload=payload, **kwargs)
        except queue.Empty as e:  # Move on
            pass
            #logger.debug('No messages in producer-consumer queue, ' + str(e))

    def synchronize(self,ident_list,**kwargs): #TODO Low level subscribed check _subscribed should be lowest level, better way to synchronize?
        queue_list = []

        for ident in ident_list:
            while True:
                try:
                    self._pop_and_handle() #TODO Ugly, find other solution
                    if self.state_machine[ident]['_subscribed'].is_set():
                        queue_list += [(ident,self.state_machine[ident][queue_name_const])]
                        break
                except KeyError:
                    pass

        return queue_list

    @_connect_wrapper
    def connect(self):
        self.ws.start()  # Start Thread
        while not self.ws.connected:
            # Wait for WebSocket Thread to establish connection
            logger.info('Establishing Connection to ' + str(self.ws.uri))
            time.sleep(1)

    @_disconnect_wrapper
    def disconnect(self):
        self.ws.close()

        if self.ws is not None and self.ws.ident:  # Check if Producer Websocket Thread is running
            self.ws.join()

    @_reconnect_wrapper
    def reconnect(self):
        self.ws.close()

    def pause(self):
        self.ws.paused = True
        logger.info('Paused: ' + str(self.ws.uri))


    def unpause(self):
        self.ws.paused = False
        logger.info('Unpaused: ' + str(self.ws.uri))


class WebSocketHelpers:
    @staticmethod
    def any_in(a, b):
        return any(i in b for i in a)

    @staticmethod
    def all_in(a, b):
        return all(i in b for i in a)

    @staticmethod
    def recursive_dict():
        rec_dict = lambda: collections.defaultdict(rec_dict)  # recursive dictionary, lambda factory
        return rec_dict()

    @staticmethod
    def filter_dict(d):
        return dict((k, v) for k, v in d.items() if v)

    @staticmethod
    def r_add(d, l):
        if len(l) == 2:
            d[l[0]] = l[1]
        else:
            key = l.pop(0)
            try:
                WebSocketHelpers.r_add(d[key], l)
            except KeyError:
                d[key] = {}
                WebSocketHelpers.r_add(d[key], l)

    @staticmethod
    def r_add_queue(d, l):
        if len(l) == 1:
            d[queue_name_const] = l[0]
        else:
            key = l.pop(0)
            try:
                WebSocketHelpers.r_add_queue(d[key], l)
            except KeyError:
                d[key] = {}
                WebSocketHelpers.r_add_queue(d[key], l)



class WebsocketManager():

    def __init__(self,**kwargs):
        self.ws_consumer = kwargs['ws_consumer']


    @classmethod
    def create(cls,websocket_uri,ws_type):
        ws = ws_type(uri=websocket_uri)
        websocket_manager = cls(ws_consumer=ws)
        return websocket_manager,ws



    def start(self,on_init,on_start):
        self.ws_consumer.start()











class ProtocolException(Exception):
    def __init__(self, msg, *args, **kwargs):
        logger.error(msg)
        Exception.__init__(self, *args, **kwargs)
