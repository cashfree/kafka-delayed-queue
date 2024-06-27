package com.cashfree.kafkaDelayedQueue.consumers;

import static com.cashfree.kafkaDelayedQueue.config.BaseKafkaConfiguration.DELAY_KAFKA_LISTENER_CONTAINER_FACTORY;
import static com.cashfree.kafkaDelayedQueue.config.BaseKafkaConfiguration.DELAY_KAFKA_TEMPLATE;

import com.cashfree.kafkaDelayedQueue.models.Kafka.KafkaDelayedQueueMessage;
import com.cashfree.kafkaDelayedQueue.utils.KafkaDelayUtil;
import com.cashfree.kafkaDelayedQueue.utils.ObjectMapperUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListeners;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LevelConsumer {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final KafkaDelayUtil kafkaDelayUtil;

  @Autowired
  public LevelConsumer(
      @Qualifier(DELAY_KAFKA_TEMPLATE) KafkaTemplate<String, String> kafkaTemplate,
      KafkaDelayUtil kafkaDelayUtil) {
    this.kafkaTemplate = kafkaTemplate;
    this.kafkaDelayUtil = kafkaDelayUtil;
  }

  @PostConstruct
  public void start() {
    log.trace("KAFKA DELAYED QUEUE  :: Level Consumer Started");
  }

  @KafkaListeners({
    @KafkaListener(
        id = "bitcheck-listener-1",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(1)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-2",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(2)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-3",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(3)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-4",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(4)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-5",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(5)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-6",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(6)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-7",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(7)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-8",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(8)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-9",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(9)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-10",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(10)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-11",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(11)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-12",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(12)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-13",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(13)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-14",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(14)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-15",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(15)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-16",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(16)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "bitcheck-listener-17",
        topics = "#{@kafkaDelayUtil.fetchTopicByLevel(17)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false")
  })
  public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic)
      throws JsonProcessingException {

    Long level = KafkaDelayUtil.fetchLevelByTopic(topic);

    log.trace(
        "KAFKA DELAYED QUEUE  :: Consuming Message : Level {} :: Message :: {}", level, message);

    KafkaDelayedQueueMessage kafkaMsg =
        ObjectMapperUtil.readValue(message, KafkaDelayedQueueMessage.class);

    log.trace(
        "KAFKA DELAYED QUEUE  :: Consuming Message : Level {} :: Message :: {}", level, message);

    boolean isBitSet = KafkaDelayUtil.checkBit(kafkaMsg.getDelay(), level - 1);

    if (!isBitSet) {
      log.trace(
          "KAFKA DELAYED QUEUE  :: Pushing Message to Level {} :: Message :: {}",
          level - 1,
          message);
      kafkaTemplate.send(kafkaDelayUtil.fetchTopicByLevel(level - 1), message);
    } else {
      log.trace(
          "KAFKA DELAYED QUEUE  :: Pushing Message to Level {} Delay :: Message :: {}",
          level,
          message);
      kafkaTemplate.send(kafkaDelayUtil.fetchDelayTopicByLevel(level), message);
    }
  }
}
