package main

import (
	"dataSource/src"
	"fmt"
	"net"
)

func main() {

	// Создание сокета для отправки сообщений
	const ADDR string = "127.0.0.1:8888"

	listener, err := net.Listen("tcp", ADDR)
	if err != nil {
		fmt.Println("Ошибка создания сокета:", err)
		return
	}
	defer listener.Close()

	server := src.NewServer()

	fmt.Println("Сокет создан, сервер запущен на адресе", ADDR)
	fmt.Println("Каждые 5 секунд публикуется случайное число")

	// Запуск горутин для отправки сообщений
	go server.BroadcastNumbers()
	go server.BroadcastNoise()

	// Прием подключений
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Ошибка подключения:", err)
			continue
		}

		go server.HandleClient(conn)
	}

}
