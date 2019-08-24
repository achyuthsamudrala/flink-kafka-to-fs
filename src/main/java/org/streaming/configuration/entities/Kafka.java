package org.streaming.configuration.entities;

import java.util.Map;
import java.util.Properties;

public class Kafka {
    private String topic;
    private Map<String, Object> consumerSettings;

    public Kafka() {
    }

    public String getTopic() {
        return this.topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Map<String, Object> getConsumerSettings() {
        return consumerSettings;
    }

    public void setConsumerSettings(Map<String, Object> consumerSettings) {
        this.consumerSettings = consumerSettings;
    }

    public Properties consumerProperties() {
        final Properties props = new Properties();
        props.putAll(consumerSettings);
        return props;
    }
}
