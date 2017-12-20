from crypt_websocket.bitfinex_websocket_v1 import BitfinexWebsocketConsumer_v1
from crypt_websocket.abstract_websocket import WebsocketManager



if __name__ == "__main__":


    bcm, bc = WebsocketManager.create(websocket_uri="wss://api.bitfinex.com/ws/2", ws_type=BitfinexWebsocketConsumer_v1)


    def on_init():
        identifier2 = bc.subscribe_to_trades(symbol='tBTCUSD')
        #identifier1 = bc.subscribe_to_ticker(symbol='tBTCUSD')
        # identifier3 = bc.subscribe_to_book(symbol='tBTCUSD')
        # identifier4 = bc.subscribe_to_candles(channel='trades', time_frame='1m', symbol='tBTCUSD')
        #q = bc.synchronize([identifier2, identifier1])

        # bc.unsubscribe(identifier1)
        #bc.disconnect()
        #bc.reconnect()


    def on_start():
        pass


    bcm.connect(on_init=on_init, on_start=on_start)


