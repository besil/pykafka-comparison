# Setup

```bash
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
$ docker-compose -f docker/docker-compose.yml up
```

# Execute performance tests
```bash
$ python -m benchmark.main -h
```
## Producer tests

### Confluent kafka client
```bash
$ python -m benchmark.main -b 127.0.0.1:9092 -t test-topic-confluent -np 1 -n 20000000 produce --confluent
```

### Kafka Python client
```bash
$ python -m benchmark.main -b 127.0.0.1:9092 -t test-topic-confluent -np 1 -n 20000000 produce --kafka-python
```

## Consumer tests

### Confluent kafka client
```bash
$ python -m benchmark.main -b 127.0.0.1:9092 -t test-topic-confluent -np 1 -n 10000000 consume -c
```

### Kafka Python client
```bash
$ python -m benchmark.main -b 127.0.0.1:9092 -t test-topic-confluent -np 1 -n 10000000 consume -kp
```
