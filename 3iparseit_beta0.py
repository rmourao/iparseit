#!/usr/bin/python2
# -*- coding: utf-8 -*-

import csv 
import pika #access rabbit
import logging
import plyvel #leveldb
import re #regular expressions
import json
import cStringIO #string aux
import time #epoch time converter
import calendar #epoch time converter
import sys

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

#plyvel.destroy_db('/home/perfteam/opt/iparseit/leveldb_headerstore')
#db = plyvel.DB('/tmp/leveldb_headerstore/', create_if_missing=True) #create database 
db = plyvel.DB('/home/perfteam/opt/iparseit/leveldb_headerstore', create_if_missing=True) #create database 

class perfmonConsumer(object):
    EXCHANGE = 'perfmon'
    EXCHANGE_TYPE = 'topic'
    QUEUE = 'otsdb'
    ROUTING_KEY = '*'

    def __init__(self, amqp_url):
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url

    def connect(self):
        LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        LOGGER.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        LOGGER.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:

            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def open_channel(self):
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        LOGGER.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def setup_exchange(self, exchange_name):
        LOGGER.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE,durable=True)

    def on_exchange_declareok(self, unused_frame):
        LOGGER.info('Exchange declared')
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        LOGGER.info('Declaring queue %s', queue_name)
        #self._channel.queue_declare(self.on_queue_declareok, queue_name,durable=True,arguments={'x-message-ttl':360000})
        self._channel.queue_declare(self.on_queue_declareok, queue_name,durable=True)

    def on_queue_declareok(self, method_frame):
        LOGGER.info('Binding %s to %s with %s',
                    self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
        self._channel.queue_bind(self.on_bindok, self.QUEUE,
                                 self.EXCHANGE, self.ROUTING_KEY)

    def on_bindok(self, unused_frame):
        LOGGER.info('Queue bound')
        self.start_consuming()

    def start_consuming(self):
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.QUEUE)

    def add_on_cancel_callback(self):
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        #LOGGER.info('Received message # %s from %s: %s',basic_deliver.delivery_tag, properties.app_id, body)
        #self.acknowledge_message(basic_deliver.delivery_tag)
   
        _body_json=json.loads(body) #parse json
        _perfmon_msg=_body_json['perfmon_msg'] #save perfmon message in variable
	_msg=cStringIO.StringIO(_perfmon_msg)
        
			#	_header example: {"hostname":"somehost","serviceid":"somesid","metrics":[
			#					{"name":"somemetric1","resource":"someresource1"},
			#					{"name":"somemetric2","resource":"someresource2"},
			#					{"name":"ASPNETApplications.RequestsSec","resource":"_LM_W123_ROOT"},
			#					{"name":"Memory.AvailableBytes","resource":""}]
									
	_hostname=str(_body_json['hostname'])
	_serviceid=_body_json['serviceid']

        if re.match('\"\(PDH-CSV 4.0\)', _perfmon_msg): #HEADER?

		_header={}
		_header['metrics']=[]
		_header['hostname']=_hostname
		_header['serviceid']=_serviceid

		for row in csv.reader(_msg):
	        	for idx,col in enumerate(row): #foreach header column

                                if idx==0:continue #timestamp column
				#print "col: %s" % col
				_nospaces=col.replace(" ","") #remove whitespaces                
				_no_bad_chars=_nospaces.translate(None, '.%#:/') # "ASP.NET", "% idle time", "w3wp#1", "Requests/Sec"
				_split_final=_no_bad_chars.split('\\') #split original metric name, example: \\VIQWRC01\.NET CLR Exceptions(w3wp)\# of Exceps Thrown
				#print "_split_final: %s" % _split_final

	
				_firstpart_metric=_split_final[3] #"NETCLRExceptions(w3wp)"
				_resource=None
				if '(' in _firstpart_metric:
					#print "in if aux %s" % aux
					aux=_firstpart_metric.split('(')
					_firstpart_metric=aux[0] #"NETCLRExceptions"
					#print "Firstpart_metric: %s" % _firstpart_metric
					_resource=aux[1][:-1] #"w3wp"

				_header['metrics'].append({'name':_firstpart_metric+'.'+_split_final[4],'resource': _resource}) #NETCLRExceptions.ofExcepsThrown"

	        _final_header=json.dumps(_header)
		db.put(_hostname,_final_header,sync=True) 
        	LOGGER.info('Header found %s', _final_header)
                #print "Header found: %s" % _final_header
		#sys.exit(0)

	else:
		_header=db.get(_hostname)
		if (_header==None):
			#print "No header found for host: %s -  Push to another queue" % _hostname #TODO
        		LOGGER.info('No header found for host: %s -  Push to another queue', _hostname)
			return

		_header=json.loads(_header)

		for datarow in csv.reader(_msg):
                        for idxi,col in enumerate(datarow): #foreach csv data field
			        #print col	
				if (idxi==0): #timestamp
					pattern = '%m/%d/%Y %H:%M:%S.%f'
					epoch = int(time.mktime(time.strptime(col, pattern))) #struct_time in local time
					#epoch = int(calendar.timegm(time.strptime(col, pattern))) #struct_time in UTC
				else:
					_metric_object=_header["metrics"][idxi-1]
					_FINAL_MSG='perfmon.'+_metric_object['name']+' '+str(epoch)+' '+col+' host='+_header['hostname']+' serviceid='+_header['serviceid']+' resource='+unicode(_metric_object['resource'])
					print _FINAL_MSG

    def acknowledge_message(self, delivery_tag):
        #LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        LOGGER.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        LOGGER.info('Stopped')

    def close_connection(self):
        LOGGER.info('Closing connection')
        self._connection.close()


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    consumer = perfmonConsumer('amqp://someuser:somepwd@somehost:5672/somevhost')
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()


if __name__ == '__main__':
    main()
