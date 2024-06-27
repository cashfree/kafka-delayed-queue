package com.cashfree.kafkaDelayedQueue;

import com.cashfree.kafkaDelayedQueue.config.BaseKafkaConfiguration;
import com.cashfree.kafkaDelayedQueue.config.KafkaListenerManager;
import com.cashfree.kafkaDelayedQueue.consumers.DelayLevelConsumer;
import com.cashfree.kafkaDelayedQueue.consumers.LevelConsumer;
import com.cashfree.kafkaDelayedQueue.utils.KafkaDelayUtil;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({
  BaseKafkaConfiguration.class,
  KafkaDelayUtil.class,
  DelayLevelConsumer.class,
  LevelConsumer.class,
  KafkaListenerManager.class
})
public @interface EnableDelayKafkaQueue {}
