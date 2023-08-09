import sys
import json
import uuid
import requests

from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


# Load configuration from INI file
config_parser = ConfigParser()
config_parser.read("config.ini")

# SR Client
sr_config = dict(config_parser["schema-registry"])
schema_registry_client = SchemaRegistryClient(sr_config)
avro_deserializer = AvroDeserializer(
    schema_registry_client,
)

# Consumer client
kafka_config = dict(config_parser["default"])
kafka_config.update(
    {
        "group.id": "cg-dylan-1",
        "client.id": uuid.uuid4().hex,
        "auto.offset.reset": "earliest",
    }
)
consumer = Consumer(kafka_config)

# Producer client
with open("schema.avsc", "r") as file:
    merged_topic_schema_str = json.dumps(json.loads(file.read()))
producer = Producer(kafka_config)
avro_serializer = AvroSerializer(
    schema_registry_client,
    merged_topic_schema_str,
)

try:
    PRODUCE_TOPIC = "AI_MODEL_RESPONSE"
    CONSUME_TOPIC = "MERGED_TOPIC_NAME"
    consumer.subscribe([CONSUME_TOPIC])

    while True:
        try:
            msg = consumer.poll(timeout=0.25)
            if msg is not None:
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    msg_value_deserialised = avro_deserializer(
                        msg.value(),
                        SerializationContext(msg.topic(), MessageField.VALUE),
                    )
                    print(f"Headers: {msg.headers()}")
                    print(f"Key: {None if msg.key() is None else msg.key().decode()}")
                    print(f"Value: {msg_value_deserialised}\n")

                    # Submit HTTP request to your machine learning model
                    ai_model_request = requests.post(
                        "http://b9c2a1a3-03bb-4eaf-8fe4-b5d710afa09c.uksouth.azurecontainer.io/score",
                        headers={},
                        json=msg_value_deserialised,
                    )
                    if ai_model_request.status_code == 200:
                        ai_model_response = ai_model_request.json()
                        # Get response and produce to another topic
                        producer.produce(
                            topic=PRODUCE_TOPIC,
                            key=msg.key(),
                            value=avro_serializer(
                                ai_model_response,
                                SerializationContext(
                                    PRODUCE_TOPIC,
                                    MessageField.VALUE,
                                ),
                            ),
                        )
                    else:
                        raise Exception(f"Error: {ai_model_request.status_code}")

        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print(
                f"{exc_type} | {exc_tb.tb_lineno} | {exc_obj}".replace(
                    ">", "&gt;"
                ).replace("<", "&lt;")
            )

finally:
    consumer.close()
