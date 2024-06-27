package com.cashfree.kafkaDelayedQueue;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CommonConstants {

  public static final String DELAY_QUEUE = ".delay-queue.";
  public static final String DELAY_QUEUE_SUFFIX = DELAY_QUEUE + "delay-level-";
  public static final String BITCHECKLEVEL_QUEUE_SUFFIX = DELAY_QUEUE + "bitcheck-level-";
}
