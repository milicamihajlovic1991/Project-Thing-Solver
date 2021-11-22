__author__ = 'Milica Mihajlovic, milicamihajlovic1991@gmail.com'
__version__ = '1.1'
__desc__ = """" This script is used to collect the data from Kafka user_topic (consumer), 
                 write to the json file (locally) and export to minio userbucket"""

from kafka import KafkaConsumer
import json
from minio import Minio
from minio.error import S3Error
import os
import pandas as pd
import datetime
import calendar
import time

client = Minio("127.0.0.1:9000", "minio", "minio123", secure = False)
if not client.bucket_exists("userbucket"):
    try:
        client.make_bucket("userbucket")
    except S3Error as identifier:
        raise

if __name__ == '__main__':
    consumer = KafkaConsumer('user_topic',
                             bootstrap_servers=['127.0.0.1:9092'],
                             auto_offset_reset = 'earliest',
                             group_id = 'consumer_group_1'  # optional
                             )
    # print('Starting a consumer')
    for user in consumer:
        dict = json.loads(user.value)
        df = pd.DataFrame([dict])
        path = '../minio/records/user' + str(calendar.timegm(time.gmtime())) + '.json'
        df.to_json(path, orient='records')
        with open(path, 'rb') as file:
            statdata = os.stat(path)
            client.put_object('userbucket', 'record ' + str(datetime.datetime.now()) + '.json', file, statdata.st_size)

    consumer.close()
