package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/geekychris/orion_mesh/clients/go/orionmesh"
)

func main() {
	queue := envOr("ORION_QUEUE_NAME", "events")
	group := envOr("ORION_QUEUE_GROUP", "go-consumer-workers")

	c, err := orionmesh.New()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	q := c.Queue(queue)
	rows, errs := q.Sub(ctx, group, 0)

	counts := map[string]int{}
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-errs:
			if e != nil {
				log.Printf("sub error: %v", e)
				return
			}
		case row, ok := <-rows:
			if !ok {
				return
			}
			msg, _ := row["msg"].(string)
			basename := msg
			if i := strings.Index(msg, "-"); i > 0 {
				basename = msg[:i]
			}
			counts[basename]++
			b, _ := json.Marshal(row)
			fmt.Printf("got: %s  counts=%v\n", b, counts)
		}
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
