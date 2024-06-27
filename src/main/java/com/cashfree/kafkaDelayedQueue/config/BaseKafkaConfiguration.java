package com.cashfree.kafkaDelayedQueue.config;

import static com.cashfree.kafkaDelayedQueue.CommonConstants.BITCHECKLEVEL_QUEUE_SUFFIX;
import static com.cashfree.kafkaDelayedQueue.CommonConstants.DELAY_QUEUE_SUFFIX;

import ch.qos.logback.core.net.ssl.SSL;
import com.cashfree.kafkaDelayedQueue.models.Kafka.BaseKafkaConfig;
import com.cashfree.kafkaDelayedQueue.utils.KafkaDelayUtil;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

@Slf4j
@Configuration
@EnableKafka
public class BaseKafkaConfiguration {
  public static final int MAX_POLL_RECORDS_CONFIG_VALUE = 500;
  public static final int FETCH_MAX_WAIT_MS_CONFIG_VALUE = 500;
  public static final int SESSION_TIMEOUT_MS_CONFIG_VALUE = 600000;
  public static final int MAX_BLOCK_MS_CONFIG_VALUE = 5000;
  public static final String DELAY_KAFKA_LISTENER_CONTAINER_FACTORY =
      "delayKafkaListenerContainerFactory";
  public static final String DELAY_KAFKA_PRODUCER_FACTORY = "delayKafkaProducerFactory";
  public static final String DELAY_KAFKA_TEMPLATE = "delayKafkaTemplate";
  public static final int POLL_TIMEOUT = 100;
  public static final String LEVEL_TO_BIT_CHECK_DELAY_TOPIC = "levelToBitCheckDelayTopic";
  public static final String LEVEL_TO_DELAY_TOPIC = "levelToDelayTopic";
  private final BaseKafkaConfig baseKafkaConfig;

  @Autowired
  public BaseKafkaConfiguration(BaseKafkaConfig baseKafkaConfig) {
    this.baseKafkaConfig = baseKafkaConfig;
  }

  @Bean(name = DELAY_KAFKA_LISTENER_CONTAINER_FACTORY)
  public ConcurrentKafkaListenerContainerFactory<String, String> delaykafkaListenerContainerFactory(
      @Qualifier("kafkaConsumerErrorHandler") CommonErrorHandler errorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    Map<String, Object> props = new HashMap<>();

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.baseKafkaConfig.getKafkaBrokers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, this.baseKafkaConfig.getConsumerGroup());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
    props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS_CONFIG_VALUE);
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, FETCH_MAX_WAIT_MS_CONFIG_VALUE);
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS_CONFIG_VALUE);
    setSSLConfigProperties(props, baseKafkaConfig.getSslEnabled());

    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
    factory.getContainerProperties().setPollTimeout(POLL_TIMEOUT);
    log.info(
        "KAFKA DELAYED QUEUE  :: DELAY CONSUMER LISTENER :: KAFKA :: INIT :: delayKafkaListenerContainerFactory");
    return factory;
  }

  @Bean(name = DELAY_KAFKA_PRODUCER_FACTORY)
  public ProducerFactory<String, String> producerFactory() {
    Map<String, Object> props = new HashMap<>();
    log.info("KAFKA DELAYED QUEUE  :: {}", this.baseKafkaConfig.getKafkaBrokers());
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.baseKafkaConfig.getKafkaBrokers());
    setSSLConfigProperties(props, baseKafkaConfig.getSslEnabled());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK_MS_CONFIG_VALUE);
    log.info("KAFKA DELAYED QUEUE :: PRODUCER CONFIG :: KAFKA :: INIT");
    return new DefaultKafkaProducerFactory<>(props);
  }

  private void setSSLConfigProperties(Map<String, Object> props, Boolean sslEnabled) {
    if (sslEnabled) {
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SSL.DEFAULT_PROTOCOL);
      props.put(
          SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.baseKafkaConfig.getTrustStoreLocation());
      props.put(
          SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.baseKafkaConfig.getTrustStorePassword());
      props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, this.baseKafkaConfig.getKeyPassword());
      props.put(
          SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, this.baseKafkaConfig.getKeyStorePassword());
      props.put(
          SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, this.baseKafkaConfig.getKeyStoreLocation());
    }
  }

  @Bean(name = DELAY_KAFKA_TEMPLATE)
  public KafkaTemplate<String, String> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  @Bean(name = LEVEL_TO_DELAY_TOPIC)
  public Map<Long, String> levelToDelayTopic() {
    Map<Long, String> levelToDelayTopic = new HashMap<>();
    for (Long i = 0L; i <= 25; i++) {
      String delayTopic = baseKafkaConfig.getTopicPrefix() + DELAY_QUEUE_SUFFIX + i;
      if (i == 0) {
        levelToDelayTopic.put(i, this.baseKafkaConfig.getDestinationTopic());
      } else {
        levelToDelayTopic.put(i, delayTopic);
      }
    }
    log.info("KAFKA DELAYED QUEUE :: DELAY TOPICS :: INIT");
    return levelToDelayTopic;
  }

  @Bean(name = LEVEL_TO_BIT_CHECK_DELAY_TOPIC)
  public Map<Long, String> levelToBitCheckDelayTopic() {
    Map<Long, String> levelToBitCheckDelayTopic = new HashMap<>();
    for (Long i = 0L; i <= 25; i++) {
      String bitCheckTopic = baseKafkaConfig.getTopicPrefix() + BITCHECKLEVEL_QUEUE_SUFFIX + i;
      if (i == 0) {
        levelToBitCheckDelayTopic.put(i, this.baseKafkaConfig.getDestinationTopic());
      } else {
        levelToBitCheckDelayTopic.put(i, bitCheckTopic);
      }
    }
    log.info("KAFKA DELAYED QUEUE :: BITCHECK DELAY TOPICS :: INIT");
    return levelToBitCheckDelayTopic;
  }

  @Bean(name = "kafkaDelayUtil")
  public KafkaDelayUtil kafkaDelayUtil() {
    return new KafkaDelayUtil(levelToBitCheckDelayTopic(), levelToDelayTopic());
  }
}
