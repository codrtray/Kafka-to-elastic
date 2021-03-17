The app shows how to use the feature of kafka. For example, how to send messages, ensure 100% delivery and increase the throughput.

Also shows different options of how to send messages to Elasticsearch.

### Used commands
#### Start kafka

        zookeeper-server-start.sh config/zookeeper.properties
        kafka-server-start.sh config/server.properties

#### Manage topics

        kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic first_topic --create --partitions 3 --replication-factor 1
        kafka-topics.sh --zookeeper 127.0.0.1:2181 --list
        kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic first_topic --describe
        kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic second_topic --delete
    
        kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic
        kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --producer-property acks=all
        kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic new_topic

        kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic
        kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --from-beginning
        kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my-first-application
        kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-first-application


        kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --to-earliest --execute --topic first_topic
