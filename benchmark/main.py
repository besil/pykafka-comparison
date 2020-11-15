import uuid
import time
import datetime
import json
from textwrap import dedent
from random import choice
from string import ascii_uppercase
from typing import List, TypedDict
import argparse
from pprint import pformat

import kafka as k
import kafka.admin as kadmin
import confluent_kafka as ck

from benchmark.utils import get_logger

log = get_logger(__name__)

class TestResult(object):
    def __init__(self, start:datetime.datetime=None, end:datetime.datetime=None, num_messages:int=1) -> None:
        self.start = start
        self.end = end
        self.num_messages = num_messages
    
    def __str__(self) -> str:
        total_messages = self.num_messages
        total_seconds = (self.end-self.start).seconds if (self.end-self.start).seconds > 0 else 1

        throughput = total_messages / total_seconds
        return dedent(f"""
            Start           : {self.start}
            End             : {self.end}
            Total messages  : {self.num_messages}
            --------------------
            Total time:     : {self.end - self.start}
            Throughput      : {throughput:.2f} msg/s
        """)


class Config(TypedDict):
    brokers: str
    topic: str
    num_partitions: int
    num_messages: int

def setup_environment(conf: Config):
    admin = k.KafkaAdminClient(bootstrap_servers=conf["brokers"])
        
    log.debug("Creating %s topic", conf["topic"])
    
    try:
        admin.create_topics([
            kadmin.NewTopic(conf["topic"], num_partitions=conf["num_partitions"], replication_factor=1)
        ])
    except Exception as e:
        pass
    admin.close()

def clean_environment(conf: Config):
    log.info("Connecting to: %s", conf["brokers"])
    admin = k.KafkaAdminClient(bootstrap_servers=[conf["brokers"]])
    for topic in admin.list_topics():
        log.info("Found topic: %s", topic)

    log.debug("Deleting %s topic", conf["topic"])
    try:
        admin.delete_topics([conf["topic"]])
    except Exception as e:
        log.warning("topic %s doesn't exists", conf["topic"])
    admin.close()

def test_producer_confluent(conf: Config) -> TestResult:
    producer = ck.Producer({
        'bootstrap.servers': conf["brokers"],
        "acks": "all"
    })

    start = datetime.datetime.now()
    for i in range(conf["num_messages"]):
        mex = ''.join(choice(ascii_uppercase) for i in range(12))
        producer.produce(conf["topic"], key=mex, value=json.dumps({"data": mex}).encode("utf-8"))
        if i % 1000 == 0:
            producer.flush()
    producer.flush()
    end = datetime.datetime.now()
    log.info("Produced %s messages", conf["num_messages"])
    return TestResult(start=start, end=end, num_messages=conf["num_messages"])
    

def test_producer_kafka_python(conf: Config) -> TestResult:
    producer = k.KafkaProducer(
        acks='all',
        bootstrap_servers=conf["brokers"],
        key_serializer=lambda k: str(k).encode('utf-8'),
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    start = datetime.datetime.now()
    for i in range(conf["num_messages"]):
        mex = ''.join(choice(ascii_uppercase) for i in range(12))
        producer.send(conf["topic"], key=mex, value={"data": mex})
        if i % 1000 == 0:
            producer.flush()
    producer.flush()
    end = datetime.datetime.now()
    log.info("Produced %s messages", conf["num_messages"])
    return TestResult(start=start, end=end, num_messages=conf["num_messages"])

def test_consumer_kafka_python(conf: Config):
    consumer = k.KafkaConsumer(
        auto_offset_reset='earliest',
        bootstrap_servers=conf["brokers"],
        group_id="kafka-python-"+str(uuid.uuid1()),
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode('utf-8'),
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    consumer.subscribe(conf["topic"])
    consumer.poll(0)
    log.info("Resetting offset to beginning...")
    consumer.seek_to_beginning()

    start=datetime.datetime.now()
    
    _counter = 0
    while _counter < conf["num_messages"]:
        data = consumer.poll(timeout_ms=500)
        for tp, messages in data.items():
            for message in messages:
                _counter += 1
                if _counter >= conf["num_messages"]:
                    break
    end=datetime.datetime.now()
    log.debug("Consumed %s messages", _counter)
    return TestResult(start=start, end=end, num_messages=_counter)

def my_on_assign(consumer: ck.Consumer, partitions: List[ck.TopicPartition]):
    for p in partitions:
        p.offset = ck.OFFSET_BEGINNING
    consumer.assign(partitions)

def test_consumer_confluent(conf: Config):
    consumer = ck.Consumer(**{'bootstrap.servers': conf["brokers"],
            'group.id': "confluent-"+str(uuid.uuid1()),
            # 'group.id': "confluent",
            'session.timeout.ms': 6000,
            'default.topic.config': {
                'auto.offset.reset': 'earliest'
            },
            'enable.auto.commit': 'true'
    })
    consumer.subscribe( [conf["topic"]], on_assign=my_on_assign)
    
    start = datetime.datetime.now()
    max_fetch_messages=500
    _counter = 0
    while _counter < conf["num_messages"]:
        messages = consumer.consume(num_messages=max_fetch_messages)
        for message in messages:
            _counter += 1
            if _counter >= conf["num_messages"]:
                break
    end=datetime.datetime.now()
    log.debug("Consumed %s messages", _counter)
    return TestResult(start=start, end=end, num_messages=_counter)


if __name__ == "__main__":
    log.info("Starting benchmark")

    parser = argparse.ArgumentParser("Library benchmark")
    parser.add_argument("--brokers", "-b", type=str, default="localhost:9092", help="List of brokers: node:port,node1:port2,...")
    parser.add_argument("--topic", "-t", type=str, required=True, help="Topic to produce or consume to")
    parser.add_argument("--num-partitions", "-np", type=int, required=False, default=1, help="Number of partitions for the given topic")
    parser.add_argument("--num-messages", "-n", type=int, required=False, default=0, help="Number of messages to produce")

    subparsers = parser.add_subparsers(title="Commands", description="Available commands", dest='cmd')

    clean_subparser = subparsers.add_parser("clean", help="Clean environment")

    produce_subparser = subparsers.add_parser("produce", help="Producer test")
    produce_subparser.add_argument("--kafka-python", "-kp", action="store_true")
    produce_subparser.add_argument("--confluent", "-c", action="store_true")
    
    consume_subparser = subparsers.add_parser("consume", help="Consumer test")
    consume_subparser.add_argument("--kafka-python", "-kp", action="store_true")
    consume_subparser.add_argument("--confluent", "-c", action="store_true")

    args = parser.parse_args()
    # log.debug("%s", pformat(args.__dict__))

    conf = Config(**args.__dict__)

    if args.cmd == "clean":
        log.info("Cleaning environment...")
        clean_environment(conf)
    elif args.cmd == "produce":
        log.info("Setup environment")
        setup_environment(conf)

        log.info("Test producer")
        if args.kafka_python:
            tr_kafka_python = test_producer_kafka_python(conf)
            log.info("Test results: \n%s", tr_kafka_python)
        elif args.confluent:
            tr_confluent = test_producer_confluent(conf)
            log.info("Test results: %s\n", tr_confluent)
        else:
            log.warning("No library selected!")
    elif args.cmd == "consume":
        log.info("Consumer test")
        if args.kafka_python:
            tr = test_consumer_kafka_python(conf)
            log.info("Test results: %s\n", tr)
        elif args.confluent:
            tr = test_consumer_confluent(conf)
            log.info("Test results: %s\n", tr)
        else: log.warning("No library selected!")
    else:
        log.warning("No command specified!")
    
    log.info("Bye!")



