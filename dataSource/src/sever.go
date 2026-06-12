package src

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

type Server struct {
	clients map[net.Conn]bool
	mutex   sync.Mutex
}

func NewServer() *Server {
	return &Server{
		clients: make(map[net.Conn]bool),
	}
}

// Обработчик входящих подключений
func (s *Server) HandleClient(conn net.Conn) {
	defer conn.Close()

	// Регистрация клиента
	s.mutex.Lock()
	s.clients[conn] = true
	s.mutex.Unlock()

	fmt.Printf("Клиент %s подключился\n", conn.RemoteAddr())

	// Ожидание отключения клиента
	for {
		// Проверка активности соединения
		_, err := conn.Read(make([]byte, 1))
		if err != nil {
			break
		}
	}

	// Удаление клиента при отключении
	s.mutex.Lock()
	delete(s.clients, conn)
	s.mutex.Unlock()

	fmt.Printf("Клиент %s отключился\n", conn.RemoteAddr())
}

func (s *Server) BroadcastNumbers() {

	for {
		// Операция выполяняется каждые 5 сек
		time.Sleep(5 * time.Second)

		randomNumber := rand.Intn(2001) - 1000 // от -1000 до 1000

		// Преобразование числа в строку
		message := strconv.Itoa(randomNumber) + "\n"
		fmt.Printf("Отправлено число: %d\n", randomNumber)

		// Отправка сообщения всем подключенным клиентам
		s.mutex.Lock()
		for conn := range s.clients {
			_, err := conn.Write([]byte(message))
			if err != nil {
				conn.Close()
				delete(s.clients, conn)
			}
		}
		s.mutex.Unlock()
	}
}

func (s *Server) BroadcastNoise() {
	for {
		// Операция выполняется каждые 6 сек
		time.Sleep(6 * time.Second)
		s.mutex.Lock()
		for conn := range s.clients {
			_, err := conn.Write([]byte("hello world"))
			if err != nil {
				conn.Close()
				delete(s.clients, conn)
			}
		}
		s.mutex.Unlock()
	}
}
