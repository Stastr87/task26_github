package main

import (
	"dataHandler/src"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"

	"github.com/joho/godotenv"
)

// Этот подход гарантирует обращение к нужному файлу, независимо от того, откуда запущена программа
func findRootDir() string {
	_, b, _, _ := runtime.Caller(0)
	currentDir := filepath.Dir(b)
	for {
		if _, err := os.Stat(filepath.Join(currentDir, "go.mod")); err == nil {
			return currentDir
		}
		parent := filepath.Dir(currentDir)
		if parent == currentDir {
			break
		}
		currentDir = parent
	}
	return "."
}

func main() {
	// Настройка отображения логов
	log.SetFlags(log.Ldate | log.Ltime)

	// Загружаем переменные из файла .env (по умолчанию ищет в текущей директории)
	rootDir := findRootDir()

	if err := godotenv.Overload(filepath.Join(rootDir, ".env")); err != nil {
		log.Fatal("No .env file found, using system environment variables")
	}

	conn, err := net.Dial("tcp", os.Getenv("MY_ADDR"))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Запуск прослушивания сокета
	listner := src.NewSocketClient(conn)
	defer listner.Stop()

	listner.Start()

	sourceChan := listner.GetData()
	doneCh := listner.Stop()

	pipeline := src.NewPipelineInt(doneCh,
		src.PrintStage,
		src.NoiseFilterStage,
		src.NegativeFilterStageInt,
		src.SpecialFilterStageInt,
		src.BufferStageInt)

	src.ReadConumer(doneCh, pipeline.Run(sourceChan))

	// Обработка ошибок пролучения данных из сокета
	go func() {
		for err := range listner.GetError() {
			log.Printf("Ошибка: %v", err)
		}
	}()

	select {} // Бесконечное ожидание

}
