package com.cashfree.kafkaDelayedQueue.consumers;

import static com.cashfree.kafkaDelayedQueue.config.BaseKafkaConfiguration.DELAY_KAFKA_LISTENER_CONTAINER_FACTORY;
import static com.cashfree.kafkaDelayedQueue.config.BaseKafkaConfiguration.DELAY_KAFKA_TEMPLATE;

import com.cashfree.kafkaDelayedQueue.models.Kafka.KafkaDelayedQueueMessage;
import com.cashfree.kafkaDelayedQueue.utils.KafkaDelayUtil;
import com.cashfree.kafkaDelayedQueue.utils.ObjectMapperUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
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
public class DelayLevelConsumer {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final KafkaDelayUtil kafkaDelayUtil;

  @Autowired
  public DelayLevelConsumer(
      @Qualifier(DELAY_KAFKA_TEMPLATE) KafkaTemplate<String, String> kafkaTemplate,
      KafkaDelayUtil kafkaDelayUtil) {
    this.kafkaTemplate = kafkaTemplate;
    this.kafkaDelayUtil = kafkaDelayUtil;
  }

  @PostConstruct
  public void start() {
    log.info("KAFKA DELAYED QUEUE  :: Delay Level Consumer Started");
  }

  @KafkaListeners({
    @KafkaListener(
        id = "delay-listener-1",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(1)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-2",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(2)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-3",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(3)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-4",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(4)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-5",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(5)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-6",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(6)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-7",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(7)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-8",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(8)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-9",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(9)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-10",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(10)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-11",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(11)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-12",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(12)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-13",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(13)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-14",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(14)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-15",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(15)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-16",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(16)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false"),
    @KafkaListener(
        id = "delay-listener-17",
        topics = "#{@kafkaDelayUtil.fetchDelayTopicByLevel(17)}",
        containerFactory = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY,
        autoStartup = "false")
  })
  public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic)
      throws InterruptedException, JsonProcessingException {

    Long level = KafkaDelayUtil.fetchLevelByTopic(topic);

    log.info("Consuming Message : Level {} : Delay :: Message :: {}", level, message);

    KafkaDelayedQueueMessage kafkaMsg =
        ObjectMapperUtil.readValue(message, KafkaDelayedQueueMessage.class);

    if (KafkaDelayUtil.getDiffInMS(kafkaMsg.getStartTime())
        > KafkaDelayUtil.fetchLevelDelay(level)) {
      kafkaMsg.setStartTime(
          kafkaMsg.getStartTime().plus(KafkaDelayUtil.fetchLevelDelay(level), ChronoUnit.MILLIS));
    } else {
      Thread.sleep(
          KafkaDelayUtil.fetchLevelDelay(level)
              - KafkaDelayUtil.getDiffInMS(kafkaMsg.getStartTime()));
      kafkaMsg.setStartTime(LocalDateTime.now());
    }
    String updatedMsg = ObjectMapperUtil.stringify(kafkaMsg);
    kafkaTemplate.send(kafkaDelayUtil.fetchTopicByLevel(level - 1), updatedMsg);
  }
}
