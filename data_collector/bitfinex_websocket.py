
from threading import Thread
import json
from abstract_websocket import AbstractWebSocket
from logging.config import fileConfig
import logging


#TODO move to __init__.py
fileConfig('logging_config.ini')
logger = logging.getLogger()



class BitfinexWebsocket(AbstractWebSocket):
    """
    Implement Bitfinex Protocol V1
    """

    def __init__(self,**kwargs):
        print(kwargs)
        super().__init__(**kwargs)

    def on_message(self,*args):
        msg_dict = json.loads(args[1])
        print(msg_dict)



    def on_close(self,*args):
        pass

    def on_open(self,*args):


        t = json.dumps({"event": "subscribe","channel":"trades","pair":"BTCUSD"})
        self.send(data=t)


    def on_error(self,*args):
        logger.error(repr(self) + ', Arguments: ' + args)























