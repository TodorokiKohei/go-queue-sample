package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	SIZE        = 100             // Queueのサイズ
	INSERT_SIZE = 7               // BatchInsertのサイズ
	TIMEOUT     = 3 * time.Second // Timeoutの時間
)

type Event struct {
	Now   time.Time
	Name  string
	Value int
}

func main() {

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	// Queueを作成する
	queue := make(chan *Event, SIZE)

	// 書き込む君
	go func(ctx context.Context) {
		// Insert処理(スリープで代用)
		insert := func(events []*Event) {
			fmt.Println("以下を書き込み中...")
			time.Sleep(500 * time.Microsecond)
			for _, event := range events {
				fmt.Printf("now=%s, client=%s, value=%d\n",
					event.Now.Format(time.RFC3339),
					event.Name,
					event.Value,
				)
			}
			fmt.Println("")
		}

		// QueueからEventを受け取って処理していく
		events := make([]*Event, 0)
	LOOP:
		for {
			select {
			case e := <-queue:
				events = append(events, e)
				if len(events) == INSERT_SIZE {
					insert(events)
					events = nil
				}
			case <-time.After(TIMEOUT):
				fmt.Println("タイムアウトしました...")
				insert(events)
				events = nil
			case <-ctx.Done():
				fmt.Println("終了前にすべてを書き込みます...")
				insert(events)
				events = nil
				break LOOP
			}
		}
	}(ctx)

	wg := &sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		// 一定時間毎にQueueにデータを挿入するgoroutineを作成(これがHandlerのイメージ)
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 10; j++ {
				event := &Event{Now: time.Now(), Name: fmt.Sprintf("client: %d", i), Value: i*10 + j}
				queue <- event
				time.Sleep(time.Duration(rand.Intn(500)+500) * time.Millisecond)
			}
			time.Sleep(TIMEOUT)
			wg.Done()
		}(i)
	}
	fmt.Print("実行待機...\n\n")
	wg.Wait()

	fmt.Println("書き込む君の終了待機..")
	cancel()
	time.Sleep(3 * time.Second)
}
