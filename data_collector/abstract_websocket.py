import websocket
from threading import Thread
import time
from datetime import datetime
import logging
from logging.config import fileConfig
import abc
import threading

#TODO move to __init__.py
fileConfig('logging_config.ini')
logger = logging.getLogger()


class AbstractWebSocket(Thread,metaclass=abc.ABCMeta):


    def __init__(self,**kwargs):

        super(AbstractWebSocket,self).__init__()

        self._uri = kwargs['uri']
        self._info = kwargs['info']


    def __repr__(self):

        _s = super().__repr__() + ', WebSocket for ' + str(self._uri)
        return _s


    def run(self):
        self.connect()


    @property
    def info(self):
        return self._info

    @property
    def uri(self):
        return self._uri


    @abc.abstractmethod
    def on_message(self,*args):
        return

    @abc.abstractmethod
    def on_error(self,*args):
        return

    @abc.abstractmethod
    def on_close(self,*args):
        return

    @abc.abstractmethod
    def on_open(self,*args):
        return

    def _connect_wrapper(func):
        """Generic Wrapper"""
        def inner(self,*args,**kwargs):

            self.start_epoch_time = time.time()
            self.start_utc_time = datetime.utcnow()
            logger.info('Start ' + repr(self) + ', Thread_ID: ' + str(threading.get_ident()))

            return func(self,*args,**kwargs)
        return inner

    def _close_wrapper(func):
        """Generic Wrapper"""
        def inner(self,*args,**kwargs):
            self.stop_epoch_time = time.time()
            self.stop_utc_time = datetime.utcnow()
            logger.info('Close ' + repr(self) + ', Thread_ID: ' + str(threading.get_ident()))
            return func(self,*args,**kwargs)
        return inner

    def _send_wrapper(func):
        """Generic Wrapper"""
        def inner(self,*args,**kwargs):
            return func(self,*args,**kwargs)
        return inner


    @_connect_wrapper
    def connect(self,*args,**kwargs):
        self.ws = websocket.WebSocketApp(self._uri, on_message=self.on_message, on_open=self.on_open,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.run_forever()

    @_close_wrapper
    def close(self,*args,**kwargs):
        self.ws.close()


    @_send_wrapper
    def send(self,*args,**kwargs):
        self.ws.send(kwargs['data'])
        






        
        
    
    





