package main

import (
  "encoding/json"
  "log"

  maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

  n := maelstrom.NewNode()

  received := map[int]bool{}

  var my_neighbors []string

  type Broadcast struct {
    Message   int  `json:"message"`
  }

  // Handle broadcast: add to the received map.
  n.Handle("broadcast", func(msg maelstrom.Message) error {
    var body Broadcast

    if err := json.Unmarshal(msg.Body, &body); err != nil {
      return err
    }
    log.Printf("Received %+v", body)

    val := int(body.Message)
    _, ok := received[val]
    if !ok { // If I didn't already have it, send it to everyone else
      log.Printf("HELLO! NEW VALUE! %d", val)
      received[val] = true
      for i := 0; i < len(my_neighbors); i++ {
        n.Send(my_neighbors[i],
          map[string]any{"type": "broadcast",
                         "message": val})
      }
    }
    return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
  })

  n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
    log.Printf("HELLO! I guess that got delivered")
    return nil
  })

  // Handle read: return everything in the map.
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


  type Topology struct {
    Topology   map[string][]string  `json:"topology"`
  }

  // Handle topology: find a list of neighbours to use.
  n.Handle("topology", func(msg maelstrom.Message) error {
    my_id := n.ID()
    everyone := n.NodeIDs()
    log.Printf("HELLO! I am %s and I know about %v", my_id, everyone)

    var body Topology
    if err := json.Unmarshal(msg.Body, &body); err != nil {
      return err
    }
    topo := body.Topology
    my_neighbors = topo[my_id]

    return n.Reply(msg, map[string]string{"type": "topology_ok"})
  })

  // Main loop
  if err := n.Run(); err != nil {
    log.Fatal("Got an error, derp:", err)
  }
}
