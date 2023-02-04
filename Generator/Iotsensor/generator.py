
#Import libraries
import json
import time
import random
import logging
import argparse
import google.auth
from datetime import datetime
from google.cloud import pubsub_v1

rand = random.random()

#Input arguments
parser = argparse.ArgumentParser(description=('Arguments for Dataflow pipeline.'))
parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
parser.add_argument(
                '--topic_name',
                required=True,
                help='PubSub topic name.')

args, opts = parser.parse_known_args()

class PubSubMessages:
    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id, topic_name):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        publish_future = self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("New data has been registered for %s", message['Product_id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")
            
rfid = ['pF8z9GBG', 'XsEOhUOT', '89x5FhyA', 'S3yG1alL', '5pz386iG']
products = ['cerdo','conejo','ternera','cordero','pollo']

# Simulates a temperature with 5% possibilities to be anormal

def temperaturaRandom():
    probabilidad= random.random()
    if probabilidad <= 0.05:
        return random.uniform(0,1) or random.uniform(5,6)
    else:
        return random.uniform(2,4)

# Generator Code

def product():

    rfid_id = random.choice(rfid)
    product_id = rfid.index(rfid_id)
    product_name = products[product_id-1]
    timestamp = str(datetime.now())
    temp_now = round(temperaturaRandom(),2)
    
    #Return values
    return {
        "Rfid_id" : rfid_id,
        "Product_id": product_id,
        "Name": product_name,
        "Timestamp": timestamp,
        "Temp_now": temp_now
        }

def run_generator(project_id, topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)
    #Publish message into the queue every 5 seconds
    try:
        while True:
            message: dict = product()
            pubsub_class.publishMessages(message)
            #it will be generated a transaction each 2 seconds
            time.sleep(5)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_generator(args.project_id, args.topic_name)
