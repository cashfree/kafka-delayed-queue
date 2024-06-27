package com.cashfree.kafkaDelayedQueue.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ObjectMapperUtil {
  private static final ObjectMapper objectMapper =
      new ObjectMapper().registerModule(new JavaTimeModule());

  static {
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static <T> String stringify(T obj) throws JsonProcessingException {
    try {
      return objectMapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      log.error("Failed to stringify object: {}. Exception: ", obj, e);
      throw e;
    }
  }

  public static <T> T readValue(String obj, Class<T> valueType) throws JsonProcessingException {
    try {
      return objectMapper.readValue(obj, valueType);
    } catch (JsonProcessingException e) {
      log.error("Failed to stringify object: {}. Exception: ", obj, e);
      throw e;
    }
  }

  public static <T, V> V convertToValue(T obj, Class<V> clazz) {
    return objectMapper.convertValue(obj, clazz);
  }
}
