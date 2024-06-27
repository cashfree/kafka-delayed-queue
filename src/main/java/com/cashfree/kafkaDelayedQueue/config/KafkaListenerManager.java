package com.cashfree.kafkaDelayedQueue.config;

import com.cashfree.kafkaDelayedQueue.models.Kafka.BaseKafkaConfig;
import com.cashfree.kafkaDelayedQueue.utils.KafkaDelayUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaListenerManager {

  private static final String DASH = "-";
  private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
  private final BaseKafkaConfig baseKafkaConfig;

  @Autowired
  public KafkaListenerManager(
      KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
      BaseKafkaConfig baseKafkaConfig) {
    this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    this.baseKafkaConfig = baseKafkaConfig;
  }

  @EventListener
  public void onApplicationEvent(ContextRefreshedEvent event) {
    startAllDelayListeners();
  }

  public void startAllDelayListeners() {
    Long maxLevel = KafkaDelayUtil.fetchIntialLevel(baseKafkaConfig.getMaxDelay());
    kafkaListenerEndpointRegistry
        .getListenerContainers()
        .forEach(
            container -> {
              try {
                long listener =
                    Long.parseLong(
                        container.getListenerId()
                            .split(DASH)[container.getListenerId().split(DASH).length - 1]);
                if (listener <= maxLevel) {
                  log.info(
                      "KAFKA DELAYED QUEUE :: STARTING CONSUMERS :: {}", container.getListenerId());
                  container.start();
                }
              } catch (Exception ignore) {
              }
            });
  }
}
