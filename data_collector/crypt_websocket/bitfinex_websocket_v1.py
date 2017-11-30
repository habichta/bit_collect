
import json
from .abstract_websocket import AbstractWebSocket
from logging.config import fileConfig
import logging
import time



#TODO move to __init__.py
fileConfig('logging_config.ini')
logger = logging.getLogger()




class BitfinexWebsocket_v1(AbstractWebSocket):
    """
    Implement Bitfinex Protocol V1
    """

    def __init__(self,**kwargs):
        super().__init__(**kwargs)






    #########################
    # Exchange Protocol
    #########################

    def bitfinex_send_protocol(self, api_key=None, secret=None, auth=False, **kwargs):
        #TODO  Authentication

        payload = json.dumps(kwargs)

        return payload

    def bitfinex_subscribe(self, channel, **r_args):
        request = {'event': 'subscribe', 'channel': channel}
        request.update(r_args)
        self.send(self.bitfinex_send_protocol, **request)

    #########################
    #Callbacks
    #########################

    def on_message(self,*args):
        msg_dict,receive_ts = json.loads(args[1]), time.time()
        print(msg_dict)


        #TODO handle: heartbeats, event_messages, responses to mesagges, reconnects


    @AbstractWebSocket._on_close
    def on_close(self,*args):
        print('Closed')


    @AbstractWebSocket._on_open
    def on_open(self,*args):
        pass


    def on_error(self,*args):
        logger.error( 'Arguments: ' + args)



















