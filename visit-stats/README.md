# visit-stats

Flink job that subscribes to Kafka topic `visit-activity` and produces aggregated visit statistics at 1 minute, 1 hour and 1 day levels. Aggregations are grouped by client IP, HTTP method and URL path. Results are written to Kafka topic `visit-stats-topic` as JSON.

Build

```
mvn -q -DskipTests package
```

Run (example, run the shaded jar produced in target):

```
java -jar target/flink-visit-stats-1.0-SNAPSHOT.jar
```

Configuration

- Kafka bootstrap address and topic names are hardcoded in `KafkaVisitStats.java` as examples. Adjust `kafkaAddr`, `inputTopic` and `outputTopic` as needed.

Notes

- The job parses incoming JSON messages into `VisitEvent`. It expects fields such as `timestamp`, `http_method`, `url`, and `client_ip`.
- Outputs include `interval` (MINUTE/HOUR/DAY), `windowStart`, `windowEnd`, `client_ip`, `http_method`, `path`, and `count`.
