package org.ing.wbaa;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.ing.wbaa.serde.Event;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import java.util.ArrayList;
import java.util.List;

public class DriverTest extends AbstractTestBase {

    @Test
    public void testDriverWithMockedSourceAndSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // values are collected in a static variable
        CollectSink.values.clear();

        Driver.buildPipeline(env, new CustomSource(), new CollectSink());
        assertThat(CollectSink.values.size(), equalTo(1));
        assertThat(CollectSink.values.get(0).getEventTime(), equalTo(123456L));
    }

    private static class CollectSink implements SinkFunction<Event> {

        // must be static
        static final List<Event> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Event value) throws Exception {
            values.add(value);
        }
    }

    private static class CustomSource implements SourceFunction<Event> {

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            sourceContext.collect(new Event(123456L, "testevent"));
        }

        @Override
        public void cancel() {
        }
    }
}
