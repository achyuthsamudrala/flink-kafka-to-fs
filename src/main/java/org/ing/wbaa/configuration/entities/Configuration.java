package org.ing.wbaa.configuration.entities;

public class Configuration {

    private Kafka kafka;
    private Environment env;

    public Configuration() {
    }

    public Kafka getKafka() {
        return kafka;
    }

    public void setKafka(Kafka kafka) {
        this.kafka = kafka;
    }

    public Environment getEnv() {
        return env;
    }

    public void setEnv(Environment env) {
        this.env = env;
    }

}
