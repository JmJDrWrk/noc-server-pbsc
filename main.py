print('@@@@@@ USING MAIN.py @@@@@@\n')
import sys
import os
import time
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaException, KafkaError
# if __name__ == '__main__':
import uvicorn
from fastapi import FastAPI, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse
from pydantic import BaseModel
from fastapi.responses import FileResponse
from time import sleep
from starlette.background import BackgroundTask
from fastapi.middleware.cors import CORSMiddleware
from starlette_context import context
from starlette_context import middleware, plugins
import os
import shutil
from threading import Thread
import json

#JSON READING
CONSUMERS = []
DONE_TASKS = []

# PUB SUB ATTEMPT
# from confluent_kafka import Producer
# topic = os.environ['bmnyjfoj-upload']
# print(topic)

from pprint import pprint

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)
app.add_middleware(
    middleware.ContextMiddleware,
    plugins=(
        plugins.ForwardedForPlugin(),
    ),
)

#------- API PART -------#


@app.get("/demuxer/cores")
async def get_demuxer_cores(req : Request):
  global CONSUMERS
  reset = req.query_params.get('reset')
  if(reset == "True"):
    print('REseted by endpoint')
    CONSUMERS = []
    #1. Notificar a los clientes conectados que deben hacer un publicar su hardware
    #Problems like looping on endpoint reload from user
    
    #PUB(json.dumps({"action":"declare_hardware_info","data":"There is no avaliable data","params":"There is no avaliable params"}))
    #2. Cuando la info esté lista se le notifica a este server
  return CONSUMERS

@app.get("/notify/newtask")
async def get_demuxer_cores():
  print('NEW TASK')
  #1. Notificar a los clientes conectados de que existe una nueva tarea en el servidor
  PUB(json.dumps({"action":"notify_new_task","data":"There is no avaliable data","params":"lock"}))
  #2. Cuando la info esté lista se le notifica a este server
  return 'Esto sería una lista de los clientes conectados'

@app.get("/")
async def root():
  return "server.author"


# ------- PURE METHODS --------- #

def alreadyExist(ip):
  for c in CONSUMERS:
    if(c['data']['network']['mip']==ip):
      try:
        CONSUMERS.remove(c)
        return False, None
      except:
        return True, c
  return False, None


def join_as_dmcore(msg_json):
  try:
    print(f'new consumer joinned')
    # user_ip = context.data["X-Forwarded-For"]
    # dmcores_ips = [cons['network']['mip'] for cons in CONSUMERS]
    # print(f" dmips {[cons['network']['mip'] for cons in CONSUMERS]}")
    exists, object = alreadyExist(msg_json['data']['network']['mip'])
    if(exists==False):
      print('append')
      CONSUMERS.append(msg_json)

    elif(exists==True):
      print('replace')
      
  except Exception as err:
    print(err)
#------- CLOUD KARAFKA -------#
CLOUDKARAFKA_BROKERS = "glider-01.srvs.cloudkafka.com:9094,glider-02.srvs.cloudkafka.com:9094,glider-03.srvs.cloudkafka.com:9094"
CLOUDKARAFKA_USERNAME = "bmnyjfoj"
CLOUDKARAFKA_PASSWORD = "w4_ZxDYDnHAHr9qcpWqiGzmFv_5K995k"
PRODUCER_CLOUDKARAFKA_TOPIC = "bmnyjfoj-tocores"
CONSUMER_CLOUDKARAFKA_TOPIC = "bmnyjfoj-tohost"
producerConf = {
    'bootstrap.servers': CLOUDKARAFKA_BROKERS,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'},
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': CLOUDKARAFKA_USERNAME,
    'sasl.password': CLOUDKARAFKA_PASSWORD
}
consumerConf = {
    'bootstrap.servers': CLOUDKARAFKA_BROKERS,
    'group.id': "%s-consumer" % CLOUDKARAFKA_USERNAME,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'},
    'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': CLOUDKARAFKA_USERNAME,
    'sasl.password': CLOUDKARAFKA_PASSWORD
}
producer_topic = PRODUCER_CLOUDKARAFKA_TOPIC
p = Producer(**producerConf)

def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d]\n' %
                         (msg.topic(), msg.partition()))
def PUB(message_data, *args, **kwargs):
  if(kwargs.get('printOnSend', None)==True): print(f'\tsent: {message_data}')
  try:
    p.produce(producer_topic, message_data, partition=0, callback=delivery_callback)
    p.flush()
  except BufferError as e:
    sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))
    p.poll(0)



## CONSUMER ## 
c = Consumer(**consumerConf)
consumer_topics = CONSUMER_CLOUDKARAFKA_TOPIC.split(",")
c.subscribe(consumer_topics)
iter = 0
def be_a_sub():
  global iter
  myiter = iter
  print(f'\t$ This is a running thread and this number {iter} must be 0')
  while True:
    #print('',end=f'Hola time {time.time()}')
    msg = c.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        # Error or event
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
        elif msg.error():
            # Error
            raise KafkaException(msg.error())
    else:
        # Proper message
        sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                         (msg.topic(), msg.partition(), msg.offset(),
                          str(msg.key())))
        print(f'\t received: {msg.value()}')
        try:
          msg_json = json.loads(str(msg.value()).strip().replace("b'","").replace("'",""))
          # SEND INSTRUCTION #
          evaluate(msg,msg_json)
          # import pip2
          # getattr(pip2, 'pan')()        
        except Exception as error:
          print('Error on consuming', error)
          
    print(f'\t$ This thread died for some reason with iter {myiter}')
  iter = iter + 33

def evaluate(msg,msg_json):
  try:
    print(f'Evaluating {msg_json["action"]}')
    action = msg_json["action"]
    data = msg_json["data"]

    if('_nodata' not in action):
      res = globals()[action](msg_json)


  except Exception as e:
    print('Exception in eval',e)
    return msg_json
  
# be_a_sub()
consumer_thread = Thread(target=be_a_sub)
consumer_thread.start()
# consumer_thread.join()

print('next')
    
if __name__ == "__main__":
  # Consumer configuration
  # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
  
  # for line in sys.stdin:
  #     try:
  #         p.produce(topic, line.rstrip(), callback=delivery_callback)
  #     except BufferError as e:
  #         sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
  #                          len(p))
  #     p.poll(0)

  # sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
  # p.flush()
  print('------------------- HANDSHAKE CLIENTS -------------------')
  PUB(json.dumps({"action":"join","data":"There is no avaliable data","params":""}),printOnSend=True)
  print('------------------- SERVER RUNNING -------------------\n')
  uvicorn.run(app, host="0.0.0.0", port=8000)