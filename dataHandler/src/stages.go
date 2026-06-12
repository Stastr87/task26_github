package src

import "log"

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
