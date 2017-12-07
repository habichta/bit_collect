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

    def bitfinex_send_protocol(self, **kwargs):
        payload = json.dumps(kwargs)

        return payload

    def bitfinex_subscribe(self,  **r_args):
        #TODO: Symbol can be without t or f prefix. This creates issue with unsubscribing since subscribed event always
        #returns with a t or f prefix but identifier will not have such a prefix.

        request = {'event': 'subscribe'}
        request.update(r_args)
        identifier = tuple(r_args.values()) #For unsubscribing
        self.send(self.bitfinex_send_protocol, **request)

        return identifier

    #Conf
    #Unsubscribe
    #Pong

    #########################
    # Producer Callbacks
    #########################

    def on_message(self, *args):
        msg_dict, receive_ts = json.loads(args[1]), time.time()
        self.pc_queue.put((receive_ts, msg_dict))

    @AbstractWebSocketProducer._on_close
    def on_close(self, *args):
        self.pc_queue.put((time.time(),AbstractWebSocketProducer.Sentinel()))


    @AbstractWebSocketProducer._on_open
    def on_open(self, *args):
        # TODO Authentication methods
        pass

    def on_error(self, *args):
        self.pc_queue.put((time.time(), AbstractWebSocketProducer.Sentinel()))
        logger.error('Arguments: ' + str(args))


class BitfinexWebsocketConsumer_v1(AbstractWebSocketConsumer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._ws = BitfinexWebsocketProducer_v1(pc_queue=self.pc_queue, **kwargs)

        ##################################
        # BitFinex Protocol
        ##################################
        self.pl_type_switch = {'event': self._handle_event, 'data': self._handle_data}
        self.pl_event_switch = {'info': self._handle_info_event,
                                'error': self._handle_error_event,
                                'subscribed': self._handle_subscribed_event,
                                'unsubscribed': self._handle_unsubscribed_event,
                                'pong': self._handle_pong_event}
        self.info_codes = {'20051':'EVT_STOP', '20060':'EVT_RESYNC_START', '20061':'EVT_RESYNC_STOP'}
        # Stop / Restart Websocket Server(please try to reconnect),
        # Refreshing data from the Trading Engine. Pause any activity (wait for 20061),
        # Done Refreshing data from the Trading Engine. Resume normal activity. Advised to
        # unsubscribe / subscribe again all channels.
        self.error_codes = {10000: 'ERR_UNK',
                             10001: 'ERR_GENERIC',
                             10008: 'ERR_CONCURRENCY',
                             10020: 'ERR_PARAMS',
                             10050: 'ERR_CONF_FAIL',
                             10100: 'ERR_AUTH_FAIL',
                             10111: 'ERR_AUTH_PAYLOAD',
                             10112: 'ERR_AUTH_SIG',
                             10113: 'ERR_AUTH_HMAC',
                             10114: 'ERR_AUTH_NONCE',
                             10200: 'ERR_UNAUTH_FAIL',
                             10300: 'ERR_SUB_FAIL',
                             10301: 'ERR_SUB_MULTI',
                             10400: 'ERR_UNSUB_FAIL',
                             11000: 'ERR_READY',
                             }

        self.book_ch_def = {'prec': 'P0',
                              'freq': 'F0',
                              'length': 25}



        self.ev_all_subscribed_fields = ['events','channel','chanId','pair']
        self.ev_add_orderbook_subscribed_fields = ['prec','freq','len']
        self.ev_add_raworderbook_subscribed_fields = ['prec','len']
        self.identifier_elements = ['channel','pair','prec', 'freq','len']



    ########################################################
    # Consumer Properties
    ########################################################
    @property
    def ws(self):
        return self._ws

    ########################################################
    # Client Interface Functions
    ########################################################

    def handle_exceptions(self):
        #TODO Check if any info codes or errors arrived that need certain actions. i.e. reconnect
        pass

    ##################################
    # Open Channels
    ##################################
    ##################################
    # Subscribing/Unsubscribing
    ##################################

    def subscribe_to_trades(self, symbol):
        return self.ws.bitfinex_subscribe(channel='trades', symbol=symbol)


    def subscribe_to_ticker(self, symbol):
        return self.ws.bitfinex_subscribe(channel='ticker', symbol=symbol)


    def subscribe_to_book(self, symbol,prec=None,freq=None,length=None):
        prec = self.book_ch_def['prec'] if prec is None else prec
        freq = self.book_ch_def['freq'] if freq is None else freq
        length = self.book_ch_def['length'] if length is None else length
        return self.ws.bitfinex_subscribe(channel='book', symbol=symbol, prec=prec,freq=freq,length=length)




    ########################################################
    # Internal Functions
    ########################################################






    ##################################
    # Handle Producer-Consumer Queue
    ##################################


    def _payload_handler(self, payload, **kwargs):  # as argument to pop_and_handle

        logger.debug(payload)

        if isinstance(payload[1],AbstractWebSocketProducer.Sentinel):
            pass
            #TODO

        else:

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
        channel, pair = str(msg['channel']),str(msg['pair'])

        for k,v in msg.items():
            self.state_machine[chanId]['_'+str(k)] = v  # Channel ID
        self.state_machine[chanId]['_ts'] = ts  # Subscrition Time stamp
        self.state_machine[chanId]['_hb'] = ts  # Time stamp last message (Channel Heartbeat)

        #Mapping (channel,pair) to

        self.state_machine[(channel,pair)] = chanId




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