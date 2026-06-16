package src

import (
	"sync"
)

type RingBuffer struct {
	// низкоуровневое хранилище буфера
	storage []int64

	// текущая позиция кольцевого буфера
	pos int

	// общий размер буфера
	size int

	// мьютекс для потокобезопасного доступа к
	// буферу.
	// Исключительный доступ нужен,
	// так так одновременно может быть вызваны
	// методы Get и Push,
	// первый - когда настало время вывести
	// содержимое буфера и очистить его,
	// второй - когда пользователь ввел новое
	// число, оба события обрабатываются разными
	// горутинами.
	mut sync.Mutex
}

// NewRingIntBuffer - создание нового буфера целых чисел
func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{make([]int64, size), -1, size, sync.Mutex{}}
}

// Push добавление нового элемента в конец буфера
// При попытке добавления нового элемента в заполненный буфер
// самое старое значение затирается
func (r *RingBuffer) Push(el int64) {
	r.mut.Lock()
	defer r.mut.Unlock()
	if r.pos == r.size-1 {
		// Сдвигаем все элементы буфера
		// на одну позицию в сторону начала
		for i := 1; i <= r.size-1; i++ {
			r.storage[i-1] = r.storage[i]
		}
		r.storage[r.pos] = el
	} else {
		r.pos++
		r.storage[r.pos] = el
	}
}

// Get - получение всех элементов буфера и его последующая очистка
func (r *RingBuffer) Get() []int64 {
	if r.pos < 0 {
		return nil
	}
	r.mut.Lock()
	defer r.mut.Unlock()
	var output []int64 = r.storage[:r.pos+1]
	// Виртуальная очистка нашего буфера
	r.pos = -1
	return output
}
