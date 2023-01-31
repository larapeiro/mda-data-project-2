
#Import libraries
import json
import time
import uuid
import random
import logging
import argparse
import google.auth
from faker import Faker
from datetime import datetime,timedelta
from google.cloud import pubsub_v1

credentials, project = google.auth.default()

fake = Faker()
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
        logging.info("New data has been registered for %s", message['Product_Id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")
            

#Generator Code
def generate_data():
    #product_id = random.choice(['pF8z9GBG', 'XsEOhUOT', '89x5FhyA', 'S3yG1alL', '5pz386iG'])
    #client_name = fake.name()
    #transaction_id = str(uuid.uuid4())
    #transaction_tmp = str(datetime.now())
    product_id = str(uuid.uuid4())
    product_name = random.choice(['yogur', 'leche','mantequilla', 'natillas', 'flan'])
    timestamp = str(datetime.now())
    #transaction_amount = fake.random_number(digits=5)
    max_temp = random.randint(7,9)
    min_temp = random.randint(2,4)
    opt_temp = random.randint(5,6)
    last_date = str(datetime.now() + timedelta(days=(random.randint(10,15))))
    #frequent_client = random.choice([True, False])
    #payment_method = "credit_card"
    #credit_card_number = fake.credit_card_number() if payment_method == "credit_card" else None 
    #email = fake.email() if frequent_client == True else None

    #Return values
    return {
        "Name": product_name,
        "Product_Id": product_id,
        "Max_Temp": max_temp,
        "Min_Temp": min_temp,
        "Opt_Temp": opt_temp,
        "Timestamp": timestamp,
        "Last_date": last_date,
        #"Is_Registered": frequent_client,
        #"Email": email,
        }

def run_generator(project_id, topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)
    #Publish message into the queue every 5 seconds
    try:
        while True:
            message: dict = generate_data()
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
