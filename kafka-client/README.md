# Demo .
### 发布（如果主题不存在, 自动创建主题）
go run pub.go -brokers=kaixina.site:9092 -topic=ttopic  -interval=500 -count=9999999 -message="Hello from Go"
### 订阅
go run sub.go -brokers=kaixina.site:9092 -topic=ttopic



# 业务 订单数据.
go run sub.go -brokers=kaixina.site:9092 -topic=order-topic
go run sub.go -brokers=kaixina.site:9092 -topic=order-stats-topic


# 业务 访问数据.
go run sub.go -brokers=kaixina.site:9092 -topic=visit-activity
go run sub.go -brokers=kaixina.site:9092 -topic="visit-stats-topic"

