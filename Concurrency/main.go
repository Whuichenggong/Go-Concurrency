package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// 使用并发加速
var maxConcurrent = 10 // 最大并发数

// 检查链接
func checkLink(url string, wg *sync.WaitGroup, sem chan struct{}) bool {

	//新增
	defer wg.Done() // 完成后通知 WaitGroup

	// 使用信号量控制并发数
	sem <- struct{}{}        // acquire semaphore
	defer func() { <-sem }() // release semaphore

	//设置超时时间
	timeout := time.Duration(6 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}

	resp, err := client.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Printf("请求链接出错: %s, 错误: %v", url, err)
		return false
	}

	defer resp.Body.Close()

	return true

}

// 分类文件
func processLinks(inputFile string, goodFile string, badFile string) {
	//打开文件
	file, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	//创建goodfile
	good, err := os.Create(goodFile)
	if err != nil {
		panic(err)
	}
	defer good.Close()
	//创建badfile
	bad, err := os.Create(badFile)
	if err != nil {
		panic(err)
	}
	defer bad.Close()

	sem := make(chan struct{}, 10) // 控制最多10个并发请求

	// WaitGroup 用来等待所有 goroutine 完成
	var wg sync.WaitGroup

	// 记录总行数，用于计算进度
	var totalLines int

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		totalLines++
	}

	file.Seek(0, 0)

	progressTicker := time.NewTicker(1 * time.Second)
	defer progressTicker.Stop()

	// 打印进度
	go func() {
		var processedLines int
		for range progressTicker.C {
			fmt.Printf("\r处理进度: %d/%d", processedLines, totalLines)
		}
	}()

	processedLines := 0
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		go func(url string) {
			isValid := checkLink(url, &wg, sem)

			// 根据有效性写入相应的文件
			if isValid {
				_, err := good.WriteString(url + "\n")
				if err != nil {
					log.Printf("写入文件出错: %v", err)
				}
			} else {
				_, err := bad.WriteString(url + "\n")
				if err != nil {
					log.Printf("写入文件出错: %v", err)
				}
			}
			// 增加已处理的行数
			processedLines++

		}(line) // 使用闭包传递参数给 goroutine
	}
	wg.Wait()
	fmt.Printf("\n处理完成，总行数: %d\n", totalLines)
}

func main() {

	// 运行任务的时间间隔，例如每小时执行一次
	ticker := time.NewTicker(1 * time.Hour) // 定期执行的时间间隔
	defer ticker.Stop()

	inputFile := "data.csv"
	goodFile := "good.csv"
	badFile := "bad.csv"

	// 定期执行任务
	for {
		startTime := time.Now()

		fmt.Println("开始执行任务...")

		processLinks(inputFile, goodFile, badFile)

		endTime := time.Now()
		duration := endTime.Sub(startTime)

		fmt.Printf("任务完成，总耗时: %v\n", duration)

		// 等待下次执行
		<-ticker.C
	}

}
