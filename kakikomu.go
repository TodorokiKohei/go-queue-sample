package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// 考慮すべきパラメータ
var (
	INSERT_SIZE = 7               // BatchInsertのサイズ
	TIMEOUT     = 2 * time.Second // Timeoutの時間
)

type Kakikomu struct {
	Queue    chan *Event
	DB       *DB
	Wg       *sync.WaitGroup
	Inserted int
}

func (k *Kakikomu) insert(events []*Event) {
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
	k.DB.Exec(query, valueArgs)
	k.Inserted += len(events)
}

func (k *Kakikomu) SaveEvent(ctx context.Context) {

	// QueueからEventを受け取って処理していく
	events := make([]*Event, 0)
LOOP:
	for {
		select {
		case e := <-k.Queue:
			events = append(events, e)
			if len(events) == INSERT_SIZE {
				k.insert(events)
				events = nil
			}
		case <-time.After(TIMEOUT):
			fmt.Println("タイムアウトしました...")
			if len(events) != 0 {
				k.insert(events)
				events = nil
			}

		case <-ctx.Done():
			fmt.Println("終了前にすべてを書き込みます...")
			if len(events) != 0 {
				k.insert(events)
				events = nil
			}
			break LOOP
		}
	}
	k.Wg.Done()
}
