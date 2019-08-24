package org.streaming;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.streaming.configuration.entities.Configuration;
import org.streaming.configuration.ConfigurationLoader;
import org.streaming.functions.EventBucketAssigner;
import org.streaming.serde.Event;
import org.streaming.serde.EventDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Driver {

    private static final Logger LOGGER = LoggerFactory.getLogger(Driver.class);
    private static final String KAFKA_SOURCE_NAME = "KAFKA-SOURCE";
    private static final String KAFKA_SINK_UID = "KAFKA-SINK";
    private static final String FILE_SYSTEM_SINK_NAME = "FILESYSTEM-SINK";
    private static final String FILE_SYSTEM_SINK_UID = "FILESYSTEM-SINK";

    public static void main(String[] args) throws Exception {

        final Configuration conf = ConfigurationLoader.load();

        LOGGER.info("Loaded pipeline with config: '{}'", conf);

        final StreamExecutionEnvironment env = getStreamExecutionEnvironment(conf);

        final FlinkKafkaConsumerBase<Event> kafkaSource = new FlinkKafkaConsumer010<>(
                conf.getKafka().getTopic(),
                new EventDeserializationSchema(),
                conf.getKafka().consumerProperties()
        )
                .setStartFromLatest()
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {
                    @Override
                    public long extractAscendingTimestamp(Event element) {
                        return element.getEventTime();
                    }
                });


        final StreamingFileSink<Event> sink = StreamingFileSink.forBulkFormat(
                new Path(conf.getEnv().getOutputDir()),
                ParquetAvroWriters
                        .forSpecificRecord(Event.class)
        )
                .withBucketAssigner(new EventBucketAssigner())
                .build();

        buildPipeline(env, kafkaSource, sink);
    }

    private static StreamExecutionEnvironment getStreamExecutionEnvironment(Configuration conf) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(conf.getEnv().getCheckpointingInterval(), CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    static void buildPipeline(StreamExecutionEnvironment env, SourceFunction<Event> kafkaSource, SinkFunction<Event> sink) throws Exception {
        DataStream<Event> source = env
                .addSource(kafkaSource)
                .name(KAFKA_SOURCE_NAME)
                .uid(KAFKA_SINK_UID);

        source
                .rebalance()
                .addSink(sink)
                .name(FILE_SYSTEM_SINK_NAME)
                .uid(FILE_SYSTEM_SINK_UID);

        env.execute("Flink Streaming : Persist to File System");
    }
}
