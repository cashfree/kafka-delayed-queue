package com.cashfree.kafkaDelayedQueue.models.Kafka;

import lombok.Data;

@Data
public class BaseKafkaConfig {
  private String kafkaBrokers;
  private String consumerGroup;
  private Boolean sslEnabled;
  private String trustStoreLocation;
  private String trustStorePassword;
  private String keyPassword;
  private String keyStorePassword;
  private String keyStoreLocation;
  private Long maxDelay;
  private String topicPrefix;
  private String destinationTopic;
}
