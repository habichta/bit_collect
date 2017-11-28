from logging.config import fileConfig
import logging
from threading import Thread
from bitfinex_websocket import BitfinexWebsocket

fileConfig('logging_config.ini')
logger = logging.getLogger()

#TODO external json file
config = {} #TRADES : which pairs


class BitfinexClient():


    def __init__(self,**ws_args):

        self.ws = BitfinexWebsocket(**ws_args)
        self.ws.start()


    def methods(self):
        pass



















def bitfinex_client_logic():
    bc = BitfinexClient(uri="wss://api.bitfinex.com/ws",info='WebSocket')


if __name__ == "__main__":
    logger.info("Starting BitFinex Collector Instance")
    main_thread = Thread(target=bitfinex_client_logic)
    main_thread.start()
    main_thread.join()