package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// 考慮すべきパラメータ
var (
	QUEUE_SIZE = 100 // Queueのサイズ
)

// 検証用パラメータ
var (
	HANDLER_NUM = 3
	SEND_NUM    = 10
)

type Event struct {
	Now   time.Time
	Name  string
	Value int
}

type DB struct{}

func (d *DB) Exec(query string, args ...any) {
	// 今回は100msec待機
	time.Sleep(100 * time.Microsecond)
}

type Handler struct {
	Queue  chan *Event
	Number int
}

func (h *Handler) Hakaru(value int) {
	event := &Event{Now: time.Now(), Name: fmt.Sprintf("handler %d", h.Number), Value: value}
	h.Queue <- event
	time.Sleep(time.Duration(rand.Intn(500)+500) * time.Millisecond)
}

func main() {

	// Queueを作成する
	queue := make(chan *Event, QUEUE_SIZE)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	DB := &DB{} // 仮のDB型

	// 書き込む君にQueueとDBを渡して実行
	wgKakikomu := &sync.WaitGroup{}
	wgKakikomu.Add(1)
	kakikomu := &Kakikomu{Queue: queue, DB: DB, Wg: wgKakikomu}
	go kakikomu.SaveEvent(ctx)

	// 検証用
	wgHandler := &sync.WaitGroup{}
	for i := 0; i < HANDLER_NUM; i++ {
		// 一定時間毎にHandlerがQueueにデータを挿入する
		wgHandler.Add(1)
		handler := Handler{Queue: queue, Number: i}
		go func() {
			for j := 0; j < SEND_NUM; j++ {
				handler.Hakaru(handler.Number*10 + j)
			}
			time.Sleep(3 * time.Second)
			wgHandler.Done()
		}()
	}
	fmt.Print("実行待機...\n\n")
	wgHandler.Wait()

	fmt.Print("書き込む君の終了待機..\n\n")
	cancel()
	wgKakikomu.Wait()

	if kakikomu.Inserted != HANDLER_NUM*SEND_NUM {
		log.Fatalf("データの損失が発生しています: send = %d, inserted = %d\n",
			kakikomu.Inserted, HANDLER_NUM*SEND_NUM)
	}
	fmt.Printf("\n%d件の書き込みに成功しました\n", kakikomu.Inserted)
}
