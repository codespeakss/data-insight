package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// 定义命令行参数
	brokers := flag.String("brokers", "localhost:9092", "Kafka broker 地址，多个地址用逗号分隔")
	topic := flag.String("topic", "", "要订阅的 Kafka 主题")
	groupID := flag.String("group", "golang-consumer-group", "Kafka 消费组 ID")
	flag.Parse()

	if *topic == "" {
		log.Fatal("必须指定订阅的 topic 参数，例如: -topic=my-topic")
	}

	// 创建 Kafka Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(*brokers, ","),
		GroupID:        *groupID,
		Topic:          *topic,
		MinBytes:       1e1,
		MaxBytes:       10e6,
		CommitInterval: 50 * time.Microsecond,
	})

	defer reader.Close()

	log.Printf("✅ 开始订阅 Kafka 主题: %s (brokers=%s)\n", *topic, *brokers)

	// 捕获中断信号，安全退出
	ctx, cancel := context.WithCancel(context.Background())
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigchan
		log.Println("🛑 收到退出信号，准备退出...")
		cancel()
	}()

	// 消费循环
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break // 用户主动退出
			}
			log.Printf("⚠️ 读取消息失败: %v\n", err)
			continue
		}
		fmt.Printf("📩 消息: topic=%s partition=%d offset=%d key=%s value=%s\n",
			m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	log.Println("✅ 已安全退出 Kafka 消费客户端。")
}
