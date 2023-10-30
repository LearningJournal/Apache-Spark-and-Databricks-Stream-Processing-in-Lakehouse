from confluent_kafka import Producer
import json
import time


class InvoiceProducer:
    def __init__(self):
        self.topic = "invoices"
        self.conf = {'bootstrap.servers': 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092',
                     'security.protocol': 'SASL_SSL',
                     'sasl.mechanism': 'PLAIN',
                     'sasl.username': 'M4DVA3BPMOUH4RSN',
                     'sasl.password': '2ONBxDYYc/YSg1+ZOdnsjFw3UxeSzjAV5vi26a8rvuNAyYGWB3EgjjAOpOQFnIoL',
                     'client.id': "prashant-laptop"}

    def delivery_callback(self, err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            key = msg.key().decode('utf-8')
            invoice_id = json.loads(msg.value().decode('utf-8'))["InvoiceNumber"]
            print(f"Produced event to : key = {key} value = {invoice_id}")

    def produce_invoices(self, producer):
        with open("data/invoices.json") as lines:
            for line in lines:
                invoice = json.loads(line)
                store_id = invoice["StoreID"]
                producer.produce(self.topic, key=store_id, value=json.dumps(invoice), callback=self.delivery_callback)
                time.sleep(0.5)
                producer.poll(1)

    def start(self):
        kafka_producer = Producer(self.conf)
        self.produce_invoices(kafka_producer)
        kafka_producer.flush()


if __name__ == "__main__":
    invoice_producer = InvoiceProducer()
    invoice_producer.start()
