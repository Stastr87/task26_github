package src

// StageByte - Стадия конвейера, обрабатывающая целые числа
type StageByte func(<-chan bool, <-chan []byte) <-chan []byte

// PipeLine - Пайплайн обработки целых чисел
type PipeLine struct {
	stages []StageByte
	done   <-chan bool
}

// NewPipelineInt - Создание пайплайна обработки целых чисел
func NewPipelineInt(done <-chan bool, stages ...StageByte) *PipeLine {
	return &PipeLine{done: done, stages: stages}
}

// runStageInt - запуск отдельной стадии конвейера
func (p *PipeLine) runStageInt(stage StageByte, sourceChan <-chan []byte) <-chan []byte {
	return stage(p.done, sourceChan)
}

// Run - Запуск пайплайна обработки целых чисел
// source - источник данных для конвейера
func (p *PipeLine) Run(source <-chan []byte) <-chan []byte {
	var c <-chan []byte = source
	for index := range p.stages {
		c = p.runStageInt(p.stages[index], c)
	}
	return c
}
