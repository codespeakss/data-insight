package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka broker 地址，多个地址用逗号分隔")
	topic := flag.String("topic", "", "要发布的 Kafka 主题")
	interval := flag.Int("interval", 1000, "消息发送间隔（毫秒）")
	count := flag.Int("count", 10, "要发送的消息数量")
	message := flag.String("message", "Hello, Kafka!", "发送的消息内容")

	flag.Parse()

	if *topic == "" {
		log.Fatal("必须指定要发布的 topic 参数，例如: -topic=my-topic")
	}

	ctx := context.Background()

	// 创建主题（如果不存在）
	err := ensureTopicExists(ctx, strings.Split(*brokers, ","), *topic)
	if err != nil {
		log.Fatalf("❌ 检查或创建主题失败: %v", err)
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(*brokers, ",")...),
		Topic:        *topic,
		RequiredAcks: kafka.RequireAll,
		Balancer:     &kafka.LeastBytes{},
	}
	defer writer.Close()

	log.Printf("🚀 开始向 Kafka 发布消息: topic=%s (brokers=%s, 间隔=%dms, 数量=%d)\n",
		*topic, *brokers, *interval, *count)

	for i := 1; i <= *count; i++ {
		msg := fmt.Sprintf("%s #%d", *message, i)
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(msg),
		})
		if err != nil {
			log.Printf("⚠️ 发送消息失败: %v\n", err)
			continue
		}
		log.Printf("✅ 已发送消息 (%d/%d): %s\n", i, *count, msg)
		time.Sleep(time.Duration(*interval) * time.Millisecond)
	}

	log.Println("🎯 所有消息已发送完成。")
}

// ensureTopicExists 检查主题是否存在，不存在则创建
func ensureTopicExists(ctx context.Context, brokers []string, topic string) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("连接 broker 失败: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("读取分区信息失败: %w", err)
	}

	for _, p := range partitions {
		if p.Topic == topic {
			log.Printf("✅ 主题已存在: %s\n", topic)
			return nil
		}
	}

	log.Printf("⚙️ 主题不存在，正在创建: %s ...\n", topic)
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = conn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("创建主题失败: %w", err)
	}

	log.Printf("🎉 成功创建主题: %s\n", topic)
	return nil
}
