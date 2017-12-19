from logging.config import fileConfig
import logging
from threading import Thread
import time
from crypt_websocket.bitfinex_websocket_v1 import BitfinexWebsocketConsumer_v1
from crypt_websocket.abstract_websocket import WebsocketManager
fileConfig('logging_config.ini')
logger = logging.getLogger()

# TODO external json file
config = {}  # TRADES : which pairs





class BitfinexClient(BitfinexWebsocketConsumer_v1):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    ##################################
    # Client Logic and Main
    ##################################

    def initialization(self):
        identifier2 = self.subscribe_to_trades(symbol='tBTCUSD')


        identifier1 = self.subscribe_to_ticker(symbol='tBTCUSD')

        # identifier3 = self.subscribe_to_book(symbol='tBTCUSD')
        # identifier4 = self.subscribe_to_candles(channel='trades', time_frame='1m', symbol='tBTCUSD')

        # self.subscribe_to_trades(pair='LTCUSD')

        q = self.synchronize([identifier2,identifier1])


        print(q)

    def execution_loop(self,**init_vars):
        pass







    # self.unsubscribe(identifier2)
        # self.unsubscribe(identifier1)
        # self.disconnect()



if __name__ == "__main__":
    logger.info("Starting BitFinex Collector Instance")
    #bc = BitfinexClient(uri="wss://api.bitfinex.com/ws/2", info='WebSocket')
    #bc.start()
    #bc.join()

    bcm, bc = WebsocketManager.create(websocket_uri="wss://api.bitfinex.com/ws/2", ws_type=BitfinexWebsocketConsumer_v1)


    def on_init():
        identifier2 = bc.subscribe_to_trades(symbol='tBTCUSD')


    def on_start():
        pass


    bcm.start(on_init=on_init, on_start=on_start)


