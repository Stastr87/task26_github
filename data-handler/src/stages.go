package src

import (
	"log"
	"os"
	"strconv"
	"time"
)

// Стадия отображения полученных данных
func PrintStage(done <-chan bool, c <-chan []byte) <-chan []byte {

	outChan := make(chan []byte)
	go func() {
		for {
			select {
			case data := <-c:
				str := string(data)
				log.Println("printStage got:", str)
				outChan <- data
			case <-done:
				return

			}
		}

	}()
	return outChan
}

// стадия, фильтрации данных от шума в виде нечисловых данных
func NoiseFilterStage(done <-chan bool, c <-chan []byte) <-chan []byte {
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
						log.Println("NoiseFilterStage return:", string(filtered))
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
func NegativeFilterStageInt(done <-chan bool, c <-chan []byte) <-chan []byte {
	convertedIntChan := make(chan []byte)
	go func() {
		for {
			select {
			case data := <-c:
				num, _ := strconv.ParseInt(string(data), 10, 64)

				if num > 0 {

					select {
					case convertedIntChan <- []byte(strconv.FormatInt(num, 10)):
						log.Println("NegativeFilterStageInt returns:", num)
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
func SpecialFilterStageInt(done <-chan bool, c <-chan []byte) <-chan []byte {
	filteredIntChan := make(chan []byte)
	go func() {
		for {
			select {
			case data := <-c:
				num, _ := strconv.ParseInt(string(data), 10, 64)
				if num != 0 && num%3 == 0 {
					select {
					case filteredIntChan <- []byte(strconv.FormatInt(num, 10)):
						log.Println("SpecialFilterStageInt returns:", num)
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
func BufferStageInt(done <-chan bool, c <-chan []byte) <-chan []byte {

	bufSize, err := strconv.Atoi(os.Getenv("MY_BUFFER_SIZE")) // Преобразует строку в int
	if err != nil {
		log.Fatal("Ошибка получения переменной bufSize")
	}
	BufferDrainIntervalInt, err := strconv.Atoi(os.Getenv("MY_BUFFER_DRAIN_INTERVAL"))
	if err != nil {
		log.Fatal("Ошибка получения переменной BufferDrainInterval")
	}
	BufferDrainInterval := time.Duration(int64(BufferDrainIntervalInt))

	bufferedIntChan := make(chan []byte)
	buffer := NewRingBuffer(bufSize)
	go func() {
		for {
			select {
			case data := <-c:
				num, _ := strconv.ParseInt(string(data), 10, 64)
				buffer.Push(num)
				log.Println("BufferStageInt >> В буфер отправлено число:", num)
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
			case <-time.After(BufferDrainInterval * time.Second):
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

// Пример функции чтения данных из пайплайна
func ReadConumer(done <-chan bool, c <-chan []byte) {
	for {
		select {
		case data := <-c:
			num, _ := strconv.ParseInt(string(data), 10, 64)
			log.Printf("ReadConumer: ... %d\n", num)
		case <-done:
			return
		}
	}
}
