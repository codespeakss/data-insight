import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.Properties;

/**
 * Flink job to consume visit-activity Kafka topic and produce aggregated visit stats
 * at 1 minute, 1 hour and 1 day levels, keyed by client_ip, http_method and URL path.
 */
public class KafkaVisitStats {
    public static void main(String[] args) throws Exception {
        final String kafkaAddr = "kaixina.site:9092";
        final String groupId = "visit-stats-group";
        final String inputTopic = "visit-activity";
        final String outputTopic = "visit-stats-topic";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka consumer props
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaAddr);
        kafkaProps.setProperty("group.id", groupId);

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                kafkaProps
        );

        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // Parse JSON => VisitEvent, assign timestamps and watermarks
        DataStream<VisitEvent> visits = kafkaStream
                .map(json -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        return mapper.readValue(json, VisitEvent.class);
                    } catch (Exception e) {
                        // parsing error -> return null and filter later
                        return null;
                    }
                })
                .filter(e -> e != null)
                .map(e -> {
                    // normalize path from url
                    try {
                        URL u = new URL(e.url);
                        e.path = u.getPath() == null || u.getPath().isEmpty() ? "/" : u.getPath();
                    } catch (Exception ex) {
                        e.path = e.url; // fallback
                    }
                    return e;
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VisitEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.timestamp)
                );

        // Build 1 minute window aggregation
        DataStream<String> minuteAgg = visits
                .keyBy(e -> keyFor(e))
                .timeWindow(Time.minutes(1))
                .process(new CountProcessWindowFunction("MINUTE"));

        // Build 1 hour window aggregation
        DataStream<String> hourAgg = visits
                .keyBy(e -> keyFor(e))
                .timeWindow(Time.hours(1))
                .process(new CountProcessWindowFunction("HOUR"));

        // Build 1 day window aggregation
        DataStream<String> dayAgg = visits
                .keyBy(e -> keyFor(e))
                .timeWindow(Time.days(1))
                .process(new CountProcessWindowFunction("DAY"));

        // Merge results
        DataStream<String> result = minuteAgg.union(hourAgg).union(dayAgg);

        // Kafka producer
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                kafkaAddr,
                outputTopic,
                new SimpleStringSchema()
        );

        result.addSink(kafkaProducer);
        result.print();

        env.execute("Job - Kafka Visit Stats");
    }

    private static String keyFor(VisitEvent e) {
        String ip = e.client_ip == null ? "-" : e.client_ip;
        String m = e.http_method == null ? "-" : e.http_method;
        String p = e.path == null ? "-" : e.path;
        return ip + "|" + m + "|" + p;
    }

    // Simple process function that counts elements in the window and emits JSON string
    private static class CountProcessWindowFunction extends ProcessWindowFunction<VisitEvent, String, String, TimeWindow> {
        private final String intervalLabel;

        public CountProcessWindowFunction(String intervalLabel) {
            this.intervalLabel = intervalLabel;
        }

        @Override
        public void process(String key, Context context, Iterable<VisitEvent> elements, Collector<String> out) throws Exception {
            long count = 0;
            for (VisitEvent e : elements) count++;

            // key format: client|method|path
            String[] parts = key.split("\\|", 3);
            String client = parts.length > 0 ? parts[0] : "-";
            String method = parts.length > 1 ? parts[1] : "-";
            String path = parts.length > 2 ? parts[2] : "-";

            WindowVisitStats s = new WindowVisitStats();
            s.interval = intervalLabel;
            s.windowStart = context.window().getStart();
            s.windowEnd = context.window().getEnd();
            s.client_ip = client;
            s.http_method = method;
            s.path = path;
            s.count = count;

            ObjectMapper mapper = new ObjectMapper();
            out.collect(mapper.writeValueAsString(s));
        }
    }

    // Visit event class mirrors incoming JSON
    public static class VisitEvent {
        public long timestamp;
        public String iso_time;
        public String http_method;
        public String host;
        public String url;
        public Object query_params;
        public Object headers;
        public String client_ip;

        // derived
        public String path;

        public VisitEvent() {}
    }

    // Output DTO for windowed stats
    public static class WindowVisitStats {
        public String interval; // MINUTE, HOUR, DAY
        public long windowStart;
        public long windowEnd;
        public String client_ip;
        public String http_method;
        public String path;
        public long count;

        public WindowVisitStats() {}
    }
}
