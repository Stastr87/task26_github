// Реализация клиента для прослушивания сокета
// с передачей данных в каналы

package src

import (
	"io"
	"log"
	"net"
	"time"
)

type SocketClient struct {
	conn      net.Conn
	dataChan  chan []byte
	errorChan chan error
	stopChan  chan bool
}

func NewSocketClient(conn net.Conn) *SocketClient {

	return &SocketClient{
		conn:      conn,
		dataChan:  make(chan []byte, 100),
		errorChan: make(chan error, 1),
		stopChan:  make(chan bool),
	}
}

func (c *SocketClient) Start() {
	go c.listening()
}

func (c *SocketClient) listening() {
	buffer := make([]byte, 8192)

	for {
		select {
		case <-c.stopChan:
			return
		default:
			c.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, err := c.conn.Read(buffer)

			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // Таймаут, продолжаем цикл
				}
				if err != io.EOF {
					c.errorChan <- err
				}
				return
			}

			if n > 0 {
				// Копирование данных для избежания перезаписи
				data := make([]byte, n)
				copy(data, buffer[:n])

				select {
				case c.dataChan <- data:
				default:
					log.Println("Канал данных переполнен")
				}
			}
		}
	}
}

// Канал с полученными данынми
func (c *SocketClient) GetData() <-chan []byte {
	return c.dataChan
}

// Канал полученных ошибок
func (c *SocketClient) GetError() <-chan error {
	return c.errorChan
}

// Сигнальный канал для завершения горутины
func (c *SocketClient) Stop() (ch <-chan bool) {
	// close(c.stopChan)
	// c.conn.Close()
	return
}
