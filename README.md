# 目标
处理网站 http 访问的海量数据，并形成统计报表 用于业务分析

# 技术实现
输入 kafka 订阅数据
输出 到 clickhouse 业务宽表
利用 AI 模型能力 识别请求是否属于攻击



# 部署
docker exec -it flink_jobmanager ./bin/flink --version  ; 


docker cp /root/flink-order-stats-1.0-SNAPSHOT.jar  flink_jobmanager:/root/order.jar ; 
docker exec -it flink_jobmanager ./bin/flink run  -c main.KafkaOrderStats /root/order.jar


docker cp /root/flink-visit-stats-1.0-SNAPSHOT.jar  flink_jobmanager:/root/visit.jar ; 
docker exec -it flink_jobmanager ./bin/flink run  -c main.KafkaOrderStats /root/visit.jar