package main

import (
        "encoding/json"
        "fmt"
        "github.com/confluentinc/confluent-kafka-go/kafka"
        "time"
        "os"
)

// 定义订单结构体
type Order struct {
        OrderID     string  `json:"orderId"`
        ProductID   string  `json:"productId"`
        ProductName string  `json:"productName"`
        Category    string  `json:"category"`
        Price       float64 `json:"price"`
        Timestamp   int64   `json:"timestamp"`
}

func main() {
        hostAddr := os.Getenv("THOST")
        if hostAddr == "" {
            hostAddr = "localhost"
        }
        broker := fmt.Sprintf("%s:9092", hostAddr)
        fmt.Println("broker =", broker)
    
        producer, err := kafka.NewProducer(&kafka.ConfigMap{
                "bootstrap.servers": broker, 
        })
        if err != nil {
                panic(fmt.Sprintf("Failed to create producer: %s", err))
        }
        defer producer.Close()

        // Kafka 主题
        topic := "order-topic"

        // 模拟发送订单数据
        for i := 1; i <= 99999999999; i++ {
                order := Order{
                        OrderID:     fmt.Sprintf("order-%04d", i),
                        ProductID:   fmt.Sprintf("product-%04d", i),
                        ProductName: fmt.Sprintf("Product Name %04d", i),
                        Category:    "CategoryA",
                        Timestamp:   time.Now().UnixMilli(),
                        Price:       float64(i) * 10.5,
                }

                // 序列化为 JSON
                orderJSON, err := json.Marshal(order)
                if err != nil {
                        fmt.Printf("Failed to serialize order: %s\n", err)
                        continue
                }

                // 构建 Kafka 消息
                message := &kafka.Message{
                        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
                        Value:          orderJSON,
                }

                // 异步发送消息
                err = producer.Produce(message, nil)
                if err != nil {
                        fmt.Printf("Failed to produce message: %s\n", err)
                } else {
                        fmt.Printf("Produced message: %s\n", string(orderJSON))
                }

                // 等待 1 秒发送下一条消息
                time.Sleep(1 * time.Second)
        }

        // 等待消息发送完成
        producer.Flush(15 * 1000)
        fmt.Println("All messages sent")
}
