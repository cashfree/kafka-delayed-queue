package com.cashfree.kafkaDelayedQueue.models.Kafka;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class KafkaDelayedQueueMessage {
  private Object message;
  private LocalDateTime pushTime;
  private LocalDateTime startTime;
  private Long delay;
}
