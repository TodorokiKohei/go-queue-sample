# go-queue-sample

`goroutine`と`channel`を利用してDBへのデータ挿入を遅延する実装  
実装イメージは以下の通り  

```mermaid
classDiagram



DB <|-- 書き込む君
書き込む君<|-- Queue

Queue<|-- ハンドラー1
Queue<|-- ハンドラー2
Queue<|-- ハンドラー3

```


