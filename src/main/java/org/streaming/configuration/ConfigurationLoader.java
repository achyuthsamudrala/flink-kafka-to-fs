package org.streaming.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import org.streaming.configuration.entities.Configuration;

public class ConfigurationLoader {

    public static Configuration load() {
        Config config = ConfigFactory.load();
        return ConfigBeanFactory.create(config.getConfig("wbaa"), Configuration.class);
    }
}
