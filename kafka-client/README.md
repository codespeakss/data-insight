# 发布（如果主题不存在, 自动创建主题）
go run pub.go -brokers=kaixina.site:9092 -topic=ttopic  -interval=500 -count=9999999 -message="Hello from Go"

# 订阅
go run sub.go -brokers=kaixina.site:9092 -topic=ttopic
