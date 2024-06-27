package com.cashfree.kafkaDelayedQueue.utils;

import static com.cashfree.kafkaDelayedQueue.config.BaseKafkaConfiguration.LEVEL_TO_BIT_CHECK_DELAY_TOPIC;
import static com.cashfree.kafkaDelayedQueue.config.BaseKafkaConfiguration.LEVEL_TO_DELAY_TOPIC;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaDelayUtil {
  private static final String DASH = "-";
  private final Map<Long, String> levelToTopic;
  private final Map<Long, String> levelToDelayTopic;

  @Autowired
  public KafkaDelayUtil(
      @Qualifier(LEVEL_TO_DELAY_TOPIC) Map<Long, String> levelToBitCheckDelayTopic,
      @Qualifier(LEVEL_TO_BIT_CHECK_DELAY_TOPIC) Map<Long, String> levelToDelayTopic) {
    this.levelToTopic = levelToBitCheckDelayTopic;
    this.levelToDelayTopic = levelToDelayTopic;
  }

  @PostConstruct
  public void start() {
    log.info("KAFKA DELAYED QUEUE  :: Delay Util Bean Created");
  }

  public static boolean checkBit(Long num, Long pos) {
    return (num & (1L << pos)) != 0;
  }

  public static Long getDiffInMS(LocalDateTime startTime) {
    return Duration.between(startTime, LocalDateTime.now()).toMillis();
  }

  public String fetchTopicByLevel(Long level) {
    return levelToTopic.get(level);
  }

  public String fetchDelayTopicByLevel(Long level) {
    return levelToDelayTopic.get(level);
  }

  public static Long fetchLevelByTopic(String topic) {
    return Long.parseLong(topic.split(DASH)[topic.split(DASH).length - 1]);
  }

  public static Long fetchLevelDelay(Long level) {
    return (long) (Math.pow(2, level - 1) * 1000);
  }

  public static Long fetchIntialLevel(Long delay) {
    return (long) ((Math.log(delay) / Math.log(2)) + 1);
  }
}
