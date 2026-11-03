package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

type Client struct {
	conn net.Conn
	send chan string
}

var clients = make(map[net.Conn]*Client)
var register = make(chan *Client)
var unregister = make(chan net.Conn)
var messages = make(chan string)

// TODO
// 1. 能不能在高 IO 占用的地方优化为无锁设计
// 2. fix 修复客户端显示 bug。
// 3. 客户端要限制发送的字数不能超过 20 字符
// 4. 客户端要限制 1 秒钟之内发送的消息数量，不能超过 3 条(optional)

func main() {
	// ip:port is a client.

	listener, err := net.Listen("tcp", "127.0.0.1:8080")

	if err != nil {
		fmt.Println("监听失败", err)
		return
	}

	defer listener.Close()
	fmt.Println("监听成功，可以连接")

	go handleEvents()

	for {
		conn, err := listener.Accept()

		if err != nil {
			fmt.Println("客户端接收失败：", err)
			return
		}
		fmt.Println("客户端可以接收：", conn.RemoteAddr())

		// 有没有意义呢？这个锁
		// 1. 加锁之前要分析要不要加，不加锁有没有问题
		// clientsMu.Lock()
		//clients[conn] = true
		// clientsMu.Unlock()

		var client = &Client{
			conn: conn,
			send: make(chan string, 900),
		}
		register <- client

		go readMessages(conn)
		go writeMessages(client)
	}
}

func writeMessages(client *Client) {
	defer fmt.Println("发送协程退出:", client.conn.RemoteAddr())
	defer client.conn.Close()
	for message := range client.send {
		err := client.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			return
		}

		if _, err := fmt.Fprintln(client.conn, message); err != nil {
			fmt.Println("发送失败:", err)
			return
		}
	}
}

func handleEvents() {
	for {
		select {
		case client := <-register:
			clients[client.conn] = client

		case conn := <-unregister:
			removeClient(conn)

		case message := <-messages:
			broadcast(message)
		}
	}
}

func readMessages(conn net.Conn) {
	defer func() {
		unregister <- conn
	}()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		message := scanner.Text()
		fmt.Println("收到消息:", message)

		sender := conn.RemoteAddr().String()
		messages <- sender + ":" + message
	}
}
func broadcast(message string) {
	for conn, client := range clients {
		select {
		case client.send <- message:
		default:
			removeClient(conn)
		}
	}
}

func removeClient(conn net.Conn) {
	client, ok := clients[conn]
	if !ok {
		return
	}
	delete(clients, conn)
	close(client.send)
	client.conn.Close()
}
