package main

import (
	"fmt"
	"log"

	"github.com/geekychris/orion_mesh/clients/go/orionmesh"
)

func main() {
	c, err := orionmesh.New()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	if _, err := c.Apply(`apiVersion: orionmesh.dev/v1
kind: Queue
metadata: { name: events }
spec: { type: work, max_age_seconds: 3600 }`); err != nil {
		log.Fatal(err)
	}

	q := c.Queue("events")
	for i := 0; i < 20; i++ {
		seq, err := q.Pub(map[string]any{"n": i, "msg": fmt.Sprintf("go-%d", i)})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("seq=%d\n", seq)
	}
	subj, _ := q.Subject()
	fmt.Printf("published 20 messages to %s\n", subj)
}
