from logging.config import fileConfig
import logging
from threading import Thread
import time
from crypt_websocket.bitfinex_websocket_v1 import BitfinexWebsocket_v1

fileConfig('logging_config.ini')
logger = logging.getLogger()

#TODO external json file
config = {} #TRADES : which pairs




class BitfinexClient():


    def __init__(self,**ws_args):

        self.ws = BitfinexWebsocket_v1(**ws_args)

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












   ##################################
    # Client Logic and Main
    ##################################

def bitfinex_client_logic():
    bc = BitfinexClient(uri="wss://api.bitfinex.com/ws",info='WebSocket')
    bc.connect()


    bc.subscribe_to_ticker(pair='BTCUSD')
    bc.subscribe_to_trades(pair='BTCUSD')

    time.sleep(5)

    bc.disconnect()


if __name__ == "__main__":
    logger.info("Starting BitFinex Collector Instance")
    main_thread = Thread(target=bitfinex_client_logic)
    main_thread.start()
    main_thread.join()