## https://developer.confluent.io/quickstart/kafka-docker/?build=apps

docker-compose up -d

# Create a topic
docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic quickstart

# write a message to the topic
docker exec --interactive --tty broker kafka-console-producer --bootstrap-server broker:9092 --topic quickstart
## type some line of text and hit Ctrl-D

# Read message from the topic
docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic quickstart --from-beginning

## Ctrl-D to exit the producer, and Ctrl-C to stop the consumer.


# kafka-py01
pip install kafka-python
## https://towardsdatascience.com/kafka-docker-python-408baf0e1088

# Stop the Kafka broker 
docker-compose down

