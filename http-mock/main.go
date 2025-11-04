package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaConfig struct {
	Addr       string
	GroupID    string
	InputTopic string
}

func GetKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Addr:       "kaixina.site:9092",
		GroupID:    "visit-stats-group",
		InputTopic: "visit-activity",
	}
}

// VisitEvent 对应Java中的结构体
type VisitEvent struct {
	HTTPMethod string `json:"http_method"`
	Host       string `json:"host"`
	URL        string `json:"url"`
	ClientIP   string `json:"client_ip"`
	Timestamp  string `json:"timestamp"`
	HTTPCode   int    `json:"http_code"` // 新增HTTP状态码字段
}

// Kafka配置
const (
	kafkaAddr  = "kaixina.site:9092"
	groupId    = "visit-stats-group"
	inputTopic = "visit-activity"
)

// IP地址池 - 这些IP将占90%的访问量
var ipPool = []string{
	"192.168.1.100",
	"192.168.1.101",
	"192.168.1.102",
	"192.168.1.103",
	"192.168.1.104",
	"10.0.0.50",
	"10.0.0.51",
	"10.0.0.52",
	"172.16.1.100",
	"172.16.1.101",
	"203.0.113.10",
	"203.0.113.11",
	"203.0.113.12",
	"198.51.100.50",
	"198.51.100.51",
}

// HTTP状态码配置
var httpCodes = []struct {
	code int
	prob float64 // 概率权重
	desc string  // 状态码描述
}{
	{200, 0.95, "OK"},
	{404, 0.015, "Not Found"},
	{500, 0.01, "Internal Server Error"},
	{302, 0.008, "Found"},
	{403, 0.007, "Forbidden"},
	{401, 0.005, "Unauthorized"},
	{429, 0.003, "Too Many Requests"},
	{503, 0.002, "Service Unavailable"},
}

func main() {
	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())

	fmt.Printf("🚀 开始生成模拟访问数据并发送到Kafka...\n\n")
	fmt.Printf("📊 IP地址池大小: %d, 将覆盖90%%的访问量\n", len(ipPool))
	fmt.Printf("📡 Kafka地址: %s\n", kafkaAddr)
	fmt.Printf("📝 主题: %s\n", inputTopic)
	fmt.Printf("🔢 HTTP状态码分布: 95%% 200 OK, 5%% 其他状态码\n\n")
	fmt.Printf(strings.Repeat("━", 80) + "\n")

	// 创建Kafka writer
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaAddr),
		Topic:    inputTopic,
		Balancer: &kafka.LeastBytes{},
	}

	defer writer.Close()

	// 统计信息
	stats := struct {
		poolIPCount   int
		randomIPCount int
		statusCount   map[int]int
		totalCount    int
	}{
		statusCount: make(map[int]int),
	}

	// 持续生成数据
	for i := 1; i <= 1000; i++ { // 生成1000条数据
		event := generateRandomVisitEvent()

		// 统计IP类型
		if isIPInPool(event.ClientIP) {
			stats.poolIPCount++
		} else {
			stats.randomIPCount++
		}

		// 统计状态码
		stats.statusCount[event.HTTPCode]++
		stats.totalCount++

		// 将结构体转换为JSON
		jsonData, err := json.Marshal(event)
		if err != nil {
			log.Printf("❌ JSON序列化错误: %v", err)
			continue
		}

		// 发送消息到Kafka
		err = writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(event.ClientIP),
				Value: jsonData,
			},
		)

		if err != nil {
			log.Printf("❌ 发送消息到Kafka失败: %v", err)
		} else {
			// 格式化输出单条记录
			printFormattedEvent(i, event)
		}

		// 每100条显示一次统计信息
		if i%100 == 0 {
			printStatistics(stats, i)
			fmt.Printf(strings.Repeat("━", 80) + "\n")
		}

		// 随机延迟 0.1-2秒
		time.Sleep(time.Duration(100+rand.Intn(1900)) * time.Millisecond)
	}

	// 最终统计
	printFinalStatistics(stats)
	fmt.Println("🎉 数据生成完成!")
}

// printFormattedEvent 格式化输出单条事件记录
func printFormattedEvent(seq int, event VisitEvent) {
	ipType := "地址池"
	if !isIPInPool(event.ClientIP) {
		ipType = "随机值"
	}

	// 根据状态码选择颜色和表情符号
	statusEmoji, statusColor := getStatusInfo(event.HTTPCode)

	// 格式化输出
	fmt.Printf("📨 %-4d │ %-6s │ %s %-6s │ %-18s │ %-25s │ %-15s │ %s%-3d%s\n",
		seq,
		ipType,
		statusEmoji,
		event.HTTPMethod,
		event.Host,
		truncateString(event.URL, 25),
		event.ClientIP,
		statusColor,
		event.HTTPCode,
		"\033[0m", // 重置颜色
	)
}

// getStatusInfo 根据HTTP状态码返回对应的表情符号和颜色
func getStatusInfo(code int) (string, string) {
	switch {
	case code == 200:
		return "✅", "\033[32m" // 绿色
	case code >= 200 && code < 300:
		return "✅", "\033[32m" // 绿色
	case code >= 300 && code < 400:
		return "🔄", "\033[33m" // 黄色
	case code >= 400 && code < 500:
		return "⚠️ ", "\033[33m" // 黄色
	case code >= 500:
		return "❌", "\033[31m" // 红色
	default:
		return "❓", "\033[37m" // 白色
	}
}

// printStatistics 打印统计信息
func printStatistics(stats struct {
	poolIPCount   int
	randomIPCount int
	statusCount   map[int]int
	totalCount    int
}, currentCount int) {
	poolPercentage := float64(stats.poolIPCount) / float64(stats.totalCount) * 100
	randomPercentage := float64(stats.randomIPCount) / float64(stats.totalCount) * 100

	fmt.Printf("\n📈 统计信息 (第%d条)\n", currentCount)
	fmt.Printf("┌%s┐\n", strings.Repeat("─", 78))

	// IP分布
	fmt.Printf("│ %-20s │ %-10s │ %-10s │ %-30s │\n", "IP类型", "数量", "百分比", "进度条")
	fmt.Printf("│%s│\n", strings.Repeat("─", 80))

	fmt.Printf("│ %-20s │ %-10d │ %-9.1f%% │ %-30s │\n",
		"地址池IP",
		stats.poolIPCount,
		poolPercentage,
		generateProgressBar(poolPercentage, 30),
	)

	fmt.Printf("│ %-20s │ %-10d │ %-9.1f%% │ %-30s │\n",
		"随机IP",
		stats.randomIPCount,
		randomPercentage,
		generateProgressBar(randomPercentage, 30),
	)

	fmt.Printf("│%s│\n", strings.Repeat("─", 80))

	// HTTP状态码分布
	fmt.Printf("│ %-20s │ %-15s │ %-10s │ %-25s │\n", "HTTP状态码", "描述", "数量", "百分比")
	fmt.Printf("│%s│\n", strings.Repeat("─", 80))

	// 获取排序后的状态码
	var codes []int
	for code := range stats.statusCount {
		codes = append(codes, code)
	}
	sort.Ints(codes)

	for _, code := range codes {
		count := stats.statusCount[code]
		percentage := float64(count) / float64(stats.totalCount) * 100
		desc := getHTTPCodeDescription(code)

		fmt.Printf("│ %-20d │ %-15s │ %-10d │ %-9.1f%% %-14s │\n",
			code,
			truncateString(desc, 15),
			count,
			percentage,
			generateProgressBar(percentage, 14),
		)
	}

	fmt.Printf("│%s│\n", strings.Repeat("─", 80))
	fmt.Printf("│ %-76s │\n", fmt.Sprintf("总计: %d 条记录", stats.totalCount))
	fmt.Printf("└%s┘\n\n", strings.Repeat("─", 78))
}

// printFinalStatistics 打印最终统计信息
func printFinalStatistics(stats struct {
	poolIPCount   int
	randomIPCount int
	statusCount   map[int]int
	totalCount    int
}) {
	fmt.Printf("\n🎯 最终统计\n")
	fmt.Printf("┌%s┐\n", strings.Repeat("─", 78))
	fmt.Printf("│ %-76s │\n", "数据生成汇总")
	fmt.Printf("│%s│\n", strings.Repeat("─", 80))

	poolPercentage := float64(stats.poolIPCount) / float64(stats.totalCount) * 100
	randomPercentage := float64(stats.randomIPCount) / float64(stats.totalCount) * 100

	fmt.Printf("│ %-20s │ %-10d │ %-9.1f%% │ %-30s │\n",
		"地址池IP",
		stats.poolIPCount,
		poolPercentage,
		generateProgressBar(poolPercentage, 30),
	)

	fmt.Printf("│ %-20s │ %-10d │ %-9.1f%% │ %-30s │\n",
		"随机IP",
		stats.randomIPCount,
		randomPercentage,
		generateProgressBar(randomPercentage, 30),
	)

	fmt.Printf("│%s│\n", strings.Repeat("─", 80))

	// HTTP状态码最终分布
	var codes []int
	for code := range stats.statusCount {
		codes = append(codes, code)
	}
	sort.Ints(codes)

	for _, code := range codes {
		count := stats.statusCount[code]
		percentage := float64(count) / float64(stats.totalCount) * 100
		desc := getHTTPCodeDescription(code)

		statusEmoji, _ := getStatusInfo(code)

		fmt.Printf("│ %s %-17d │ %-15s │ %-10d │ %-9.1f%% %-14s │\n",
			statusEmoji,
			code,
			truncateString(desc, 15),
			count,
			percentage,
			generateProgressBar(percentage, 14),
		)
	}

	fmt.Printf("│%s│\n", strings.Repeat("─", 80))
	fmt.Printf("│ %-76s │\n", fmt.Sprintf("🎊 总计生成: %d 条记录", stats.totalCount))
	fmt.Printf("└%s┘\n", strings.Repeat("─", 78))
}

// generateProgressBar 生成进度条
func generateProgressBar(percentage float64, length int) string {
	barLength := int(percentage * float64(length) / 100)
	if barLength > length {
		barLength = length
	}

	bar := strings.Repeat("█", barLength)
	empty := strings.Repeat("░", length-barLength)

	return bar + empty
}

// getHTTPCodeDescription 获取HTTP状态码描述
func getHTTPCodeDescription(code int) string {
	for _, httpCode := range httpCodes {
		if httpCode.code == code {
			return httpCode.desc
		}
	}
	return "Unknown"
}

// truncateString 截断字符串并添加省略号
func truncateString(s string, maxLength int) string {
	if len(s) <= maxLength {
		return s
	}
	return s[:maxLength-3] + "..."
}

// generateRandomVisitEvent 生成随机的访问事件，90%使用地址池IP，10%使用随机IP
func generateRandomVisitEvent() VisitEvent {
	httpMethods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	hosts := []string{
		"www.example.com",
		"api.example.com",
		"blog.example.com",
		"shop.example.com",
		"admin.example.com",
		"static.example.com",
		"cdn.example.com",
	}

	urls := []string{
		"/",
		"/home",
		"/about",
		"/contact",
		"/products",
		"/api/v1/users",
		"/api/v1/orders",
		"/blog/post/123",
		"/login",
		"/dashboard",
		"/settings",
		"/cart",
		"/checkout",
		"/search",
		"/images/logo.png",
	}

	// 生成客户端IP - 90%概率使用地址池IP，10%概率使用随机IP
	var clientIP string
	if rand.Float64() < 0.9 { // 90%的概率
		// 从地址池中随机选择一个IP
		clientIP = ipPool[rand.Intn(len(ipPool))]
	} else { // 10%的概率
		// 生成随机IP地址
		clientIP = generateRandomIP()
	}

	return VisitEvent{
		HTTPMethod: httpMethods[rand.Intn(len(httpMethods))],
		Host:       hosts[rand.Intn(len(hosts))],
		URL:        urls[rand.Intn(len(urls))],
		ClientIP:   clientIP,
		Timestamp:  time.Now().Format(time.RFC3339),
		HTTPCode:   getRandomHTTPCode(), // 新增：生成随机HTTP状态码
	}
}

// getRandomHTTPCode 根据概率分布生成HTTP状态码
func getRandomHTTPCode() int {
	r := rand.Float64()
	cumulativeProb := 0.0

	for _, codeProb := range httpCodes {
		cumulativeProb += codeProb.prob
		if r <= cumulativeProb {
			return codeProb.code
		}
	}

	// 默认返回200
	return 200
}

// generateRandomIP 生成随机IP地址
func generateRandomIP() string {
	// 避免生成私有地址段的IP
	firstOctet := rand.Intn(223) + 1 // 1-223，跳过0, 224-255（多播和保留）

	// 跳过私有地址段
	if firstOctet == 10 || firstOctet == 127 {
		return generateRandomIP() // 递归调用直到生成非私有IP
	}
	if firstOctet == 172 {
		secondOctet := rand.Intn(16) + 16 // 172.16.0.0 - 172.31.255.255
		if secondOctet >= 16 && secondOctet <= 31 {
			return generateRandomIP() // 递归调用直到生成非私有IP
		}
	}
	if firstOctet == 192 && rand.Intn(2) == 0 { // 50%概率是192.168.x.x
		return generateRandomIP() // 递归调用直到生成非私有IP
	}

	return fmt.Sprintf("%d.%d.%d.%d",
		firstOctet,
		rand.Intn(256),
		rand.Intn(256),
		rand.Intn(256),
	)
}

// isIPInPool 检查IP是否在地址池中
func isIPInPool(ip string) bool {
	for _, poolIP := range ipPool {
		if poolIP == ip {
			return true
		}
	}
	return false
}
