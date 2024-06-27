# Kafka Delayed Queue for Spring and Java

Kafka Delayed Queue is a library that facilitates delayed message processing in Kafka, tailored for Spring applications. This library leverages Kafka topics and scheduled tasks to efficiently manage and process delayed messages, ensuring high-throughput and reliable message handling.

## Features

- **Seamless Integration:** Easily integrate with Spring applications.
- **Configurable Delays:** Set customizable delay intervals for message processing.
- **High Throughput:** Optimized for handling large volumes of messages.
- **Scalable and Reliable:** Built on a robust architecture for scalability and reliability.

## Prerequisites

- **Java:** Version 8 or higher.
- **Apache Kafka:** Version 2.0.0 or higher.
- **Spring Boot:** Version 2.0 or higher.

## Getting Started

### 1. Create Kafka Topics
Create Kafka topics based on the formula: `2 * log2(MaxDelay)`. Ensure these topics are available in your Kafka cluster.

```shell
#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <kafka_bin_path> <prefix> <n>"
    exit 1
fi

# Assign the arguments to variables
KAFKA_BIN_PATH=$1
PREFIX=$2
N=$3

# Loop through 1 to n and create topics
for (( i=1; i<=N; i++ ))
do
    DELAY_QUEUE_TOPIC="${PREFIX}.delay-queue.delay-level-${i}"
    BITCHECK_QUEUE_TOPIC="${PREFIX}.delay-queue.bitcheck-level-${i}"

    # Create delay queue topic
    "${KAFKA_BIN_PATH}/kafka-topics.sh" --create --topic "${DELAY_QUEUE_TOPIC}" --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    if [ $? -ne 0 ]; then
        echo "Failed to create topic: ${DELAY_QUEUE_TOPIC}"
    else
        echo "Successfully created topic: ${DELAY_QUEUE_TOPIC}"
    fi

    # Create bitcheck queue topic
    "${KAFKA_BIN_PATH}/kafka-topics.sh" --create --topic "${BITCHECK_QUEUE_TOPIC}" --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
    if [ $? -ne 0 ]; then
        echo "Failed to create topic: ${BITCHECK_QUEUE_TOPIC}"
    else
        echo "Successfully created topic: ${BITCHECK_QUEUE_TOPIC}"
    fi
done

```

```commandline
./create_delay_kafka_topics.sh /usr/local/kafka/bin xyz 5
```

### 2. Install the Library
Download and install the JAR file from the Kafka Delayed Queue releases.

### 3. Enable Delayed Kafka Queue
Annotate your main application class or configuration class with `@EnableDelayKafkaQueue` to enable the delayed queue functionality.

### 4. Configure Kafka
Create a BaseKafkaConfig bean in your configuration file. Configure the topic prefix, maximum delay, and destination topic and Other Resources.

```java
@EnableKafka
@EnableDelayKafkaQueue
@Configuration
public class KafkaConsumerConfig extends BaseKafkaConfiguration {

  private final String kafkaBrokers;
  private final int kafkaConcurrency;
  private final String consumerGroup;
  private final Boolean sslEnabled;
  private final String trustStoreLocation;
  private final String trustStorePassword;
  private final String keyPassword;
  private final String keyStorePassword;
  private final String keyStoreLocation;
 ....

  @Bean
  public BaseKafkaConfig library() {
    BaseKafkaConfig baseKafkaConfig = new BaseKafkaConfig();
    baseKafkaConfig.setKafkaBrokers(this.kafkaBrokers);
    baseKafkaConfig.setConsumerGroup(this.consumerGroup);
    baseKafkaConfig.setSslEnabled(Boolean.FALSE);
    baseKafkaConfig.setKeyPassword(this.keyPassword);
    baseKafkaConfig.setKeyStoreLocation(this.keyStoreLocation);
    baseKafkaConfig.setKeyStorePassword(trustStorePassword);
    baseKafkaConfig.setTrustStoreLocation(trustStoreLocation);
    baseKafkaConfig.setTrustStorePassword(trustStorePassword);
    baseKafkaConfig.setMaxDelay(1000L);
    baseKafkaConfig.setTopicPrefix("xyz");
    baseKafkaConfig.setDestinationTopic("xyz.delay-queue.final");
    return baseKafkaConfig;
  }
}
```

### 5. Start Pushing Messages
Use the kafkaTemplate bean to start pushing messages into the queue.

```java
@Service
public class MessageProducer {
    
    
    private final KafkaTemplate<String, String> kafkaTemplate;

    private final KafkaDelayUtil kafkaDelayUtil;

    @Autowired
    public MessageProducer(
            KafkaTemplate<String, String> kafkaTemplate, KafkaDelayUtil kafkaDelayUtil) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaDelayUtil = kafkaDelayUtil;
    }

    public void sendMessage(String message, long delay) {
        KafkaDelayedQueueMessage kafkaMsg = new KafkaDelayedQueueMessage();
        kafkaMsg.setDelay(duration.toSeconds());
        kafkaMsg.setMessage(message);
        kafkaMsg.setStartTime(LocalDateTime.now());
        kafkaMsg.setPushTime(LocalDateTime.now());

        Long level = KafkaDelayUtil.fetchIntialLevel(duration.toSeconds());
        

        kafkaTemplate.send(
                kafkaDelayUtil.fetchTopicByLevel(level),
                ObjectMapperUtil.stringify(kafkaMsg, Boolean.TRUE));
        
    }
}

```


### 5. Start Listening Messages in the destination topic

```java
@Slf4j
@Component
public class KafkaReconWorker {

    @KafkaListener(
            topics = "xyz.delay-queue.final")
    public void listen(String message) {

        KafkaDelayedQueueMessage delayedQueueMsg =
                ObjectMapperUtil.readValue(message, KafkaDelayedQueueMessage.class, Boolean.FALSE);

        System.out.println("Message Received :: " + delayedQueueMsg.getMessage());
        ....
    }
}

```





