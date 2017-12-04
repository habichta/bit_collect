import json
from .abstract_websocket import AbstractWebSocketProducer, AbstractWebSocketConsumer,WebSocketHelpers
from logging.config import fileConfig
import logging
import hmac
import hashlib
import time

from pprint import pprint

# TODO move to __init__.py
fileConfig('logging_config.ini')
logger = logging.getLogger()
logger.setLevel('DEBUG')


class BitfinexWebsocketProducer_v1(AbstractWebSocketProducer):
    """
    Implement Bitfinex Protocol V1, Producer side
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    #########################
    # Exchange Protocol
    #########################

    def bitfinex_send_protocol(self, api_key=None, secret=None, auth=False, **kwargs):
        payload = json.dumps(kwargs)

        return payload

    def bitfinex_subscribe(self, channel, **r_args):
        request = {'event': 'subscribe', 'channel': channel}
        request.update(r_args)
        self.send(self.bitfinex_send_protocol, **request)

    #########################
    # Producer Callbacks
    #########################

    def on_message(self, *args):
        msg_dict, receive_ts = json.loads(args[1]), time.time()
        self.pc_queue.put((receive_ts, msg_dict))

    @AbstractWebSocketProducer._on_close
    def on_close(self, *args):
        # TODO Sentinel?
        print('Closed')

    @AbstractWebSocketProducer._on_open
    def on_open(self, *args):
        # TODO Authentication methods
        pass

    def on_error(self, *args):
        # TODO Sentinel?
        logger.error('Arguments: ' + str(args))


class BitfinexWebsocketConsumer_v1(AbstractWebSocketConsumer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.ws = BitfinexWebsocketProducer_v1(pc_queue=self.pc_queue, **kwargs)

        ##################################
        # BitFinex Protocol
        ##################################
        self.pl_type_switch = {'event': self._handle_event, 'data': self._handle_data}
        self.pl_event_switch = {'info': self._handle_info_event,
                                'error': self._handle_error_event,
                                'subscribed': self._handle_subscribed_event,
                                'unsubscribed': self._handle_unsubscribed_event,
                                'pong': self._handle_pong_event}
        self.info_codes = ['20051', '20060', '20061']
        # Stop / Restart Websocket Server(please try to reconnect),
        # Refreshing data from the Trading Engine. Pause any activity (wait for 20061),
        # Done Refreshing data from the Trading Engine. Resume normal activity. Advised to
        # unsubscribe / subscribe again all channels.

        self.error_codes = ['10000', '10001','10300','10301','10302','10400','10401']
        # Unkown event, Unkown pair, Subscription failed(generic), Already subscribed, Unknown channel,
        # Unsubscription failed(generic), Not subscribed
        
        self.auth_error_codes = ['10100', '10101', '10102', '10103', '10104', '10201']
        # Authentication failure(generic), Already authenticated, Authentication Payload Error,
        # Authentication Signature Error, Authentication HMAC Error, Not authenticated

        self.ev_all_subscribed_fields = ['events','channel','chanId','pair']
        self.ev_add_orderbook_subscribed_fields = ['prec','freq','len']
        self.ev_add_raworderbook_subscribed_fields = ['prec','len']



    ########################################################
    # Client Interface Functions
    ########################################################
    def connect(self):
        self.ws.start()
        while not self.ws.connected:
            # Wait for WebSocket Thread to establish connection
            logger.info('Establishing Connection to ' + str(self.ws.uri))
            time.sleep(1)

    def disconnect(self):
        self.ws.close()
        # TODO reset state machine
        if self.ws is not None and self.ws.ident:  # Check if Producer Websocket Thread is running
            self.ws.join()

    def handle_exceptions(self):
        # Check if any info codes or errors arrived that need certain actions. i.e. reconnect
        pass

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



    ########################################################
    # Internal Functions
    ########################################################
    ##################################
    # Handle Producer-Consumer Queue
    ##################################


    def _payload_handler(self, payload, **kwargs):  # as argument to pop_and_handle

        logger.debug(payload)

        self.state_machine['hb'] = payload[0] #Update global Heartbeat with timestamp

        if isinstance(payload[1], dict):  # events are dictionaries
            self.pl_type_switch['event'](payload=payload, **kwargs)
        else:  # data is a list
            self.pl_type_switch['data'](payload=payload, **kwargs)

    def _handle_event(self, payload, **kwargs):

        event_type = payload[1]['event']
        try:
            self.pl_event_switch[event_type](payload=payload, **kwargs)
        except KeyError as e:
            logger.error(str(e) + ', got: ' + str(event_type))
        except Exception as e:
            logger.error(str(e) + ', got: ' + str(event_type))


    def _handle_data(self, **kwargs):
        pass

    ##################################
    # Consumer Callbacks
    ##################################
    ##################################
    # Bitfinex Events
    ##################################

    def _handle_info_event(self, payload, **kwargs):

        msg = payload[1] #Json message
        ts = payload[0] #Receive time stamp

        if 'version' in msg:
            self.state_machine['version']['_v'] = msg['version']
            self.state_machine['version']['_ts'] = ts

        elif 'code' in msg:
            info_code = str(msg['code'])
            if info_code in self.info_codes:
                try:
                    info_message =  str(msg['msg'])
                except KeyError:
                    info_message = None

                self.state_machine['info'][info_code]['_ts'] = ts
                self.state_machine['info'][info_code]['_msg'] = info_message

                logger.info(str(ts)+ ': ' + info_code+ ': ' + info_message)
            else:
                logger.error(str(ts)+': Unknown info code: ', str(info_code))
        else:
            logger.error(str(ts)+': Unknown info message: ' + str(msg))

    def _handle_error_event(self, payload, **kwargs):
        print(payload)

    def _handle_subscribed_event(self, payload, **kwargs):

        ts,msg = payload[0],payload[1]  # Json message
        chanId = str(msg['chanId'])

        for k,v in msg.items():
            self.state_machine[chanId]['_'+str(k)] = v  # Channel ID
        self.state_machine[chanId]['_ts'] = ts  # Subscrition Time stamp
        self.state_machine[chanId]['_hb'] = ts  # Time stamp last message (Channel Heartbeat)

        #Mapping chan,pair to id

    def _handle_unsubscribed_event(self, payload, **kwargs):
        pass



    def _handle_pong_event(self, payload, **kwargs):
        print(payload)


    def _check_protocol_sanity(self):
        pass


    ##################################
    # Bitfinex Data
    ##################################



    ##################################
    # Authentication/Security
    ##################################

    def _create_authentication_payload(self,api_secret,api_key):

        nonce = int(time.time() * 1000000)
        auth_payload = 'AUTH{}'.format(nonce)
        signature = hmac.new(
            api_secret.encode(),
            msg=auth_payload.encode(),
            digestmod=hashlib.sha384
        ).hexdigest()

        auth_payload = {
            'apiKey': api_key,
            'event': 'auth',
            'authPayload': auth_payload,
            'authNonce': nonce,
            'authSig': signature
        }

        return auth_payload