# Data+AI Summit 2024 - overcoming unit tests challenges - Scala example

```
cd docker/
mkdir -p /tmp/dais2024/devices/input
docker-compose down --volumes; docker-compose up
```

```
rm -rf /tmp/dais24_checkpoint/
```

```
--kafkaBootstrapServers localhost:9094 --kafkaInputTopic visits \
--kafkaOutputTopic sessions --devicesTableLocation /tmp/dais2024/devices \
--checkpointLocation /tmp/dais24_checkpoint/
```

```
docker exec dais_24_kafka kafka-console-consumer.sh --topic sessions --bootstrap-server localhost:9092
```