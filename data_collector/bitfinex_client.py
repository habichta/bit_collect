from crypt_websocket.bitfinex_websocket_v1 import BitfinexWebsocketConsumer_v1
from crypt_websocket.abstract_websocket import WebsocketManager


#TODO Testing, Database, Define Data Shape/Model

if __name__ == "__main__":


    bcm, bc = WebsocketManager.create(websocket_uri="wss://api.bitfinex.com/ws/2", ws_type=BitfinexWebsocketConsumer_v1)


    def on_run():

        #identifier2 = bc.subscribe_to_trades(symbol='tBTCUSD')
        #identifier3 = bc.subscribe_to_book(symbol='tBTCUSD')
        cid = bc.ping()
        identifier1 = bc.subscribe_to_ticker(symbol='tBTCUSD')
        # identifier3 = bc.subscribe_to_book(symbol='tBTCUSD')
        # identifier4 = bc.subscribe_to_candles(channel='trades', time_frame='1m', symbol='tBTCUSD')
        # q = bc.synchronize([identifier2, identifier1])

        #bc.synchronize([identifier2, identifier3])



        #print(bc.state_machine)
        #bc.unsubscribe(identifier2)



        #while bcm.running:
         #   pass


        '''
        if bc.get_notification_state(identifier2):
            bc.unsubscribe(identifier2)
        '''


    bcm.connect(on_run=on_run)


