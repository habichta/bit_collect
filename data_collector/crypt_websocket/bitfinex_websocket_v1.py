
import json
from .abstract_websocket import AbstractWebSocketProducer,AbstractWebSocketConsumer
from logging.config import fileConfig
from threading import Thread
import logging
import time



#TODO move to __init__.py
fileConfig('logging_config.ini')
logger = logging.getLogger()




class BitfinexWebsocketProducer_v1(AbstractWebSocketProducer):
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


    @AbstractWebSocketProducer._on_close
    def on_close(self,*args):
        print('Closed')


    @AbstractWebSocketProducer._on_open
    def on_open(self,*args):
        pass


    def on_error(self,*args):
        logger.error( 'Arguments: ' + str(args))




class BitfinexWebsocketConsumer_v1(AbstractWebSocketConsumer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.ws = BitfinexWebsocketProducer_v1(pc_queue=self.pc_queue,**kwargs)




    ##################################
    # Connect/Disconnect
    ##################################
    def connect(self):
        self.ws.start()
        while not self.ws.connected:
            #Wait for WebSocket Thread to establish connection
            print('Establishing Connection to ', self.ws.uri)
            time.sleep(1)

    def disconnect(self):
        self.ws.close()
        if self.ws is not None and self.ws.ident:
            self.ws.join()


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
















