import logging
import argparse
import datetime
import certstream
from elasticsearch import Elasticsearch

def _process_callback(message, context):
    # extract and convert timestamps from epoch to datetime
    seen_datetime = datetime.datetime.fromtimestamp(message['data']['seen'])
    not_before_datetime = datetime.datetime.fromtimestamp(message['data']['leaf_cert']['not_before'])
    not_after_datetime = datetime.datetime.fromtimestamp(message['data']['leaf_cert']['not_after'])

    logging.debug('translating seen to [%s]', seen_datetime)
    logging.debug('translating not_before to [%s]', not_before_datetime)
    logging.debug('translating not_after_time to [%s]', not_after_datetime)

    # inject converted timestamps
    message['data']['seen_time'] = seen_datetime
    message['data']['leaf_cert']['not_before_datetime'] = not_before_datetime
    message['data']['leaf_cert']['not_after_datetime'] = not_after_datetime

    # send certstream data to elasticsearch
    response = es.index(index='certstream-data', body=message)
    logging.debug('elastic search response [%s]', response)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A transport script for moving data from certstream to Elasticsearch.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-es', default='http://localhost:9200', help='elasticsearch server to send certificate information to')
    parser.add_argument('-cs', default='wss://certstream.calidog.io', help='certstream server to obtain certificate information from')
    parser.add_argument('-v', dest='logging_level', action='store_const', const=logging.DEBUG, default=logging.WARNING, help='enable debug logging')
    parser.add_argument('--process_heartbeats', action='store_false', help='start processing heartbeat messages')
    args = parser.parse_args()

    # setup logging
    logging.basicConfig(format='%(levelname)-8s %(message)s', level=args.logging_level)

    # connect to elastic search
    logging.info('ingesting to the elastic search endpoint [%s]', args.cs)
    es = Elasticsearch([args.es])

    # connect to and start processing certstream certificates
    logging.info('monitoring the certstream server [%s]', args.cs)
    logging.info('heartbeat status [%s]', args.process_heartbeats)
    certstream.listen_for_events(_process_callback, args.cs, skip_heartbeats=args.process_heartbeats)