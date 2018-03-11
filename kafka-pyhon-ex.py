from kafka import *

mykafka = KafkaClient("localhost:9092")

producer = SimpleProducer(mykafka)

producer.send_messages("test", "mymessage")
producer.send_messages("test", "mymessage 2")
producer.send_messages("test", "mymessage 3")
producer.send_messages("test", "mymessage 4")
