package main

import (
  //"encoding/json"
  //"fmt"
  "log"

  uuid "github.com/google/uuid"
  maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

  n := maelstrom.NewNode()

  // Test input: {"body": {"type": "generate"}}
  n.Handle("generate", func(msg maelstrom.Message) error {
    //fmt.Println("Running generate on", msg.Body)
    id := uuid.New()
    //fmt.Println("Generated id", id.String())
    //if err := json.Unmarshal(msg.Body, &body); err != nil {
    //  return err
    //}
    body := map[string]string{}
    body["type"] = "generate_ok"
    body["id"] = id.String()
    return n.Reply(msg, body)
  })


  if err := n.Run(); err != nil {
    log.Fatal(err)
  }

}
