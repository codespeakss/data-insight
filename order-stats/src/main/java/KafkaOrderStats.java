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

import java.time.Duration;
import java.util.Properties;

public class KafkaOrderStats {
    public static void main(String[] args) throws Exception {
        final String kafkaAddr = "kaixina.site:9092";
        
        final String groupId = "order-stats-group";
        final String inputTopic = "order-topic";
        final String outputTopic = "order-stats-topic";

        // 1. 初始化 Flink 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置 Kafka 消费者
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaAddr);
        kafkaProps.setProperty("group.id", groupId);

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new SimpleStringSchema(),
                kafkaProps
        );

        // 3. 添加 Kafka 消费者
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // 4. 转换数据流并分配时间戳和水位线
        DataStream<Order> ordersStream = kafkaStream
                .map(json -> {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readValue(json, Order.class);
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.timestamp)
                );

        // 5. 按商品类别统计窗口内消费总额
        DataStream<Tuple2<String, Double>> resultStream = ordersStream
                .keyBy(order -> order.category)
                .timeWindow(Time.seconds(10))
                .process(new ProcessWindowFunction<Order, Tuple2<String, Double>, String, TimeWindow>() {
                    @Override
                    public void process(String category, Context context, Iterable<Order> elements, Collector<Tuple2<String, Double>> out) {
                        double total = 0;
                        for (Order order : elements) {
                            total += order.price;
                        }
                        out.collect(new Tuple2<>(category, total));
                    }
                });

        // 6. 转换结果为 JSON 字符串
        DataStream<String> jsonResultStream = resultStream.map(tuple -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(new OrderStats(tuple.f0, tuple.f1));
        });

        // 7. 将结果输出到 Kafka 
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                kafkaAddr,       // Kafka 服务器地址
                outputTopic,        // 输出主题
                new SimpleStringSchema()    // 序列化 Schema
        );
        jsonResultStream.addSink(kafkaProducer);

        // 8. 同时打印到控制台，方便调试
        jsonResultStream.print();

        // 9. 启动作业
        env.execute("Job - Kafka Order Stats");
    }

    // 定义订单类
    public static class Order {
        public String orderId;
        public String productId;
        public String productName;
        public String category;
        public double price;
        public long timestamp;

        public Order() {}
    }

    // 定义统计结果类
    public static class OrderStats {
        public String category;
        public double totalAmount;

        public OrderStats() {}

        public OrderStats(String category, double totalAmount) {
            this.category = category;
            this.totalAmount = totalAmount;
        }
    }
}
