package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"
	"unicode/utf8"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:8080")

	if err != nil {
		fmt.Println("服务器连接失败：", err)
		return
	}

	defer conn.Close()

	fmt.Println("欢迎来到聊天室")
	go receive(conn)
	input := bufio.NewScanner(os.Stdin)

	var sentTimes []time.Time

	for input.Scan() {
		message := input.Text()

		if utf8.RuneCountInString(message) > 20 {
			fmt.Println("消息不能超过20个字符，请重新输入")
			continue
		}

		now := time.Now()

		for len(sentTimes) > 0 && now.Sub(sentTimes[0]) >= time.Second {
			sentTimes = sentTimes[1:]
		}

		if len(sentTimes) >= 3 {
			fmt.Println("发送太快了，1秒内最多发送3条消息")
			continue
		}

		if _, err := fmt.Fprintln(conn, message); err != nil {
			fmt.Println("发送失败：", err)
			return
		}

		sentTimes = append(sentTimes, time.Now())
	}

	if err := input.Err(); err != nil {
		fmt.Println("读取输入失败：", err)
	}
}

func receive(conn net.Conn) {
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("接收失败：", err)
	}
	fmt.Println("与服务端断开连接")
}
