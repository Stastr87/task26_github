package main

import (
	"dataHandler/src"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"
)

func main() {

	// Размер кольцевого буфера
	const bufferSize int = 10

	// Интервал очистки кольцевого буфера
	const bufferDrainInterval time.Duration = 15 * time.Second

	// Создание соединения
	const ADDR string = "127.0.0.1:8888"
	conn, err := net.Dial("tcp", ADDR)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Запуск прослушивания сокета
	listner := src.NewSocketClient(conn)
	// defer listner.Stop()

	listner.Start()

	// Стадия отображения полученных данных
	printStage := func(done <-chan bool, c <-chan []byte) <-chan []byte {

		outChan := make(chan []byte)
		go func() {
			for {
				select {
				case data := <-c:
					str := string(data)
					fmt.Println("printStage got:", str)
					outChan <- data
				case <-done:
					return

				}
			}

		}()
		return outChan
	}

	// стадия, фильтрации данных от шума в виде нечисловых данных
	noiseFilterStage := func(done <-chan bool, c <-chan []byte) <-chan []byte {
		filteredChan := make(chan []byte)
		go func() {
			for {
				select {
				case data := <-c:
					filtered := make([]byte, 0, len(data))
					for _, b := range data {
						if (b >= '0' && b <= '9') || b == '-' {
							filtered = append(filtered, b)
						}
					}

					if len(filtered) > 0 {
						select {
						case filteredChan <- filtered:
						case <-done:
							return
						}

					}
				case <-done:
					return
				}

			}

		}()
		return filteredChan
	}

	// стадия, фильтрующая отрицательные числа
	negativeFilterStageInt := func(done <-chan bool, c <-chan []byte) <-chan []byte {
		convertedIntChan := make(chan []byte)
		go func() {
			for {
				select {
				case data := <-c:
					num, _ := strconv.ParseInt(string(data), 10, 64)

					if num > 0 {

						select {
						case convertedIntChan <- []byte(strconv.FormatInt(num, 10)):
						case <-done:
							return
						}
					}
				case <-done:
					return
				}
			}
		}()
		return convertedIntChan
	}

	// стадия, фильтрующая числа, не кратные 3
	specialFilterStageInt := func(done <-chan bool, c <-chan []byte) <-chan []byte {
		filteredIntChan := make(chan []byte)
		go func() {
			for {
				select {
				case data := <-c:
					num, _ := strconv.ParseInt(string(data), 10, 64)
					if num != 0 && num%3 == 0 {
						select {
						case filteredIntChan <- []byte(strconv.FormatInt(num, 10)):
						case <-done:
							return
						}
					}
				case <-done:
					return
				}
			}
		}()
		return filteredIntChan
	}

	// стадия буферизации
	bufferStageInt := func(done <-chan bool, c <-chan []byte) <-chan []byte {
		bufferedIntChan := make(chan []byte)
		buffer := src.NewRingBuffer(bufferSize)
		go func() {
			for {
				select {
				case data := <-c:
					num, _ := strconv.ParseInt(string(data), 10, 64)
					buffer.Push(num)
				case <-done:
					return
				}
			}
		}()
		// В этой стадии есть вспомогательная горутина,
		// выполняющая просмотр буфера с заданным интервалом
		// времени -
		// bufferDrainInterval
		go func() {
			for {
				select {
				case <-time.After(bufferDrainInterval):
					bufferData := buffer.Get()
					// Если в кольцевом буфере что-то есть -
					// выводим
					// содержимое построчно

					for _, data := range bufferData {
						select {
						case bufferedIntChan <- []byte(strconv.FormatInt(data, 10)):
						case <-done:
							return
						}
					}

				case <-done:
					return
				}
			}
		}()
		return bufferedIntChan
	}

	sourceChan := listner.GetData()
	doneCh := listner.Stop()

	pipeline := src.NewPipelineInt(doneCh,
		printStage,
		noiseFilterStage,
		negativeFilterStageInt,
		specialFilterStageInt,
		bufferStageInt)

	// Потребитель данных от пайплайна
	consumer := func(done <-chan bool, c <-chan []byte) {
		for {
			select {
			case data := <-c:
				num, _ := strconv.ParseInt(string(data), 10, 64)
				fmt.Printf("consumer: в буфер сохраненено число... %d\n", num)
			case <-done:
				return
			}
		}
	}

	consumer(doneCh, pipeline.Run(sourceChan))

	// Обработка ошибок пролучения данных из сокета
	go func() {
		for err := range listner.GetError() {
			log.Printf("Ошибка: %v", err)
		}
	}()

	select {} // Бесконечное ожидание

}
