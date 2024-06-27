package com.cashfree.kafkaDelayedQueue.models.Kafka;

import lombok.Data;

@Data
public class DelayedQueueMsg {
  private String message;
  private Long delay;
}
