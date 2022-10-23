package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// 考慮すべきパラメータ
var (
	SIZE        = 100             // Queueのサイズ
	INSERT_SIZE = 7               // BatchInsertのサイズ
	TIMEOUT     = 2 * time.Second // Timeoutの時間
)

type Event struct {
	Now   time.Time
	Name  string
	Value int
}

type DB struct{}

func (d *DB) Exec(query string, args ...any) {
	// ここでInsertされる
	time.Sleep(500 * time.Microsecond)
}

type Kakikomu struct {
	queue chan *Event
	db    *DB
}

func (k *Kakikomu) SaveEvent(ctx context.Context) {
	// Insert処理(スリープで代用)
	insert := func(events []*Event) {
		fmt.Println("以下を書き込み中...")
		valueStrings := make([]string, 0)
		valueArgs := make([]interface{}, 0)
		number := 1
		for _, event := range events {
			fmt.Printf("now=%s, client=%s, value=%d\n",
				event.Now.Format(time.RFC3339),
				event.Name,
				event.Value,
			)
			valueStrings = append(valueStrings, fmt.Sprintf(" ($%d, $%d, $%d)", number, number+1, number+2))
			valueArgs = append(valueArgs, event.Now)
			valueArgs = append(valueArgs, event.Name)
			valueArgs = append(valueArgs, event.Value)
			number += 3
		}
		query := fmt.Sprintf("INSERT INTO users (at, name, value) VALUES %s;", strings.Join(valueStrings, ","))
		fmt.Printf("%s\n\n", query)
		k.db.Exec(query, valueArgs)
	}

	// QueueからEventを受け取って処理していく
	events := make([]*Event, 0)
LOOP:
	for {
		select {
		case e := <-k.queue:
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
}

type Handler struct {
	queue  chan *Event
	number int
}

func (h *Handler) Hakaru(value int) {
	event := &Event{Now: time.Now(), Name: fmt.Sprintf("handler %d", h.number), Value: value}
	h.queue <- event
	time.Sleep(time.Duration(rand.Intn(500)+500) * time.Millisecond)
}

func main() {

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	// Queueを作成する
	queue := make(chan *Event, SIZE)

	// 書き込む君を実行
	kakikomu := &Kakikomu{queue: queue}
	go kakikomu.SaveEvent(ctx)

	wg := &sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		// 一定時間毎にHandlerがQueueにデータを挿入する
		wg.Add(1)
		handler := Handler{queue: queue, number: i}
		go func() {
			for j := 0; j < 10; j++ {
				handler.Hakaru(handler.number*10 + j)
			}
			time.Sleep(3 * time.Second)
			wg.Done()
		}()
	}
	fmt.Print("実行待機...\n\n")
	wg.Wait()

	fmt.Print("書き込む君の終了待機..\n\n")
	cancel()
	time.Sleep(3 * time.Second)
}
