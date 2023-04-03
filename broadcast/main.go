package main

import (
  "encoding/json"
//  "errors"
  "log"
  "sync"

  maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Outbox struct {

}




func main() {

  n := maelstrom.NewNode()

  var mu sync.Mutex
  received := map[int]bool{}  // protected by mu
  var my_neighbors []string   // protected by mu
  outbox := map[string][]int{}   // protected by mu

  // This is my broadcast message id, unrelated to the value I'm sending.
  // I send the same message id to multiple nodes
  next_message_id := 0          // protected by mu

  // TODO: implement the resend functionality
  // TODO: only need a separate broadcast id if I keep doing individual messages


  type Broadcast struct {
    Message   int  `json:"message"`
    MsgId     int  `json:"msg_id"`
  }

  /*type RPCBody struct {
    Type  string  `json:"type"`
    MsgID int     `json:"msg_id"`
    InReplyTo int `json:"in_reply_to"`
  }*/

  // Handle RPC response
  // TODO: Currently a no-op
  handle_rpc_resp := func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
      return err
    }
    log.Printf("%d", body["in_reply_to"])
    log.Printf("OK MESSAGE WAS %+v", body)
    return nil
  }

  // Handle broadcast message: add to the received map.
  n.Handle("broadcast", func(msg maelstrom.Message) error {
    var body Broadcast

    if err := json.Unmarshal(msg.Body, &body); err != nil {
      return err
    }
    val := int(body.Message)
    received_message_id := int(body.MsgId)

    mu.Lock()
    _, ok := received[val]
    if !ok { // If I didn't already have it, send it to everyone else

      // TODO: extract this re-broadcast
      received[val] = true
      for i := 0; i < len(my_neighbors); i++ {
        outbox[my_neighbors[i]] = append(outbox[my_neighbors[i]], val)
      }
    }

    for i := 0; i < len(my_neighbors); i++ {
      neighbor := my_neighbors[i]

      // TODO: currently sending once for everything in the outbox; write a
      // batch function.
      for j := 0; i < len(outbox[neighbor]); i++ {
        val := outbox[neighbor][j]
        n.RPC(neighbor,
              map[string]any{"type": "broadcast",
                             "message": val,
                             "msg_id": next_message_id},
                             handle_rpc_resp)
      }
    }
    next_message_id += 1
    mu.Unlock()

    // Reply that we successfully received this one.
    return n.Reply(msg, map[string]any{
       "type": "broadcast_ok",
       "in_reply_to": received_message_id} )
  })

  n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
    //log.Printf("HELLO! I guess that got delivered")
    // This message just contains map[in_reply_to:0 type:broadcast_ok]
    /*var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
      return err
    }
    log.Printf("OK MESSAGE WAS %+v", body)
    */
    //return errors.New("OMG ERROR")
    return nil
  })

  // Handle read: return everything in the map.
  n.Handle("read", func(msg maelstrom.Message) error {
    var values []int
    // TODO: how is performance? Make a copy instead?
    mu.Lock()
    for k, _ := range received {
      values = append(values, k)
    }
    mu.Unlock()

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
    mu.Lock()
    my_neighbors = topo[my_id]
    mu.Unlock()

    return n.Reply(msg, map[string]string{"type": "topology_ok"})
  })

  // Main loop
  if err := n.Run(); err != nil {
    log.Fatal("Got an error, derp:", err)
  }
}
