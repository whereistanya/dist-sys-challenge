package main

import (
  "encoding/json"
  "log"

  maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

  n := maelstrom.NewNode()

  received := map[int]bool{}

  type Broadcast struct {
    Message   int  `json:"message"`
  }

  n.Handle("broadcast", func(msg maelstrom.Message) error {
    var body Broadcast

    if err := json.Unmarshal(msg.Body, &body); err != nil {
      return err
    }
    received[int(body.Message)] = true
    return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
  })


  n.Handle("read", func(msg maelstrom.Message) error {
    var values []int
    for k, _ := range received {
      values = append(values, k)
    }

    return n.Reply(msg, map[string]any{
      "type": "read_ok",
      "messages": values,
    })
  })

  n.Handle("topology", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
      return err
    }
    return n.Reply(msg, map[string]string{"type": "topology_ok"})
  })

  if err := n.Run(); err != nil {
    log.Fatal("Got an error, derp:", err)
  }

}
