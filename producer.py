import pandas as pd
from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

dataset = pd.read_csv('pre_train4.csv')
dataset = dataset.to_dict('records')

for x in dataset:
    data = {'data' : x, 'player': 'ICE'}
    print(data)
    producer.send('space-wars-events', key='SPACE_WARS'.encode('utf-8'), value=data)
    sleep(2)
   