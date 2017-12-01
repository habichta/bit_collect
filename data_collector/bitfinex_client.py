from logging.config import fileConfig
import logging
from threading import Thread
import time
from crypt_websocket.bitfinex_websocket_v1 import BitfinexWebsocketConsumer_v1

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

    def initialize_connection(self):
        #TODO all subscriptions or auth .. etc.
        pass


    def run(self):
        self.connect()

        # self.subscribe_to_ticker(pair='BTCUSD')
        self.subscribe_to_trades(pair='BTCUSD')

        while True:
            self.pop_and_handle(handle_func=self._payload_handler)
            time.sleep(0.5)

        self.disconnect()


if __name__ == "__main__":
    logger.info("Starting BitFinex Collector Instance")
    bc = BitfinexClient(uri="wss://api.bitfinex.com/ws", info='WebSocket')
    bc.start()
    bc.join()
