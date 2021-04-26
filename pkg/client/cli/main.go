package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"

	"github.com/rancher/kine/pkg/client"
	endpoint2 "github.com/rancher/kine/pkg/endpoint"
)

var (
	endpoint string
)

func main() {
	flag.StringVar(&endpoint, "endpoint", "http://localhost:2379", "ETCD Endpoint")
	flag.Parse()

	err := run(flag.Args())
	if err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	c, err := client.New(endpoint2.ETCDConfig{
		Endpoints: []string{endpoint},
	})
	if err != nil {
		return err
	}
	defer c.Close()

	switch args[0] {
	case "list":
		key := args[1]
		rev := 0
		if len(args) > 2 {
			rev, err = strconv.Atoi(args[2])
			if err != nil {
				return err
			}
		}
		vals, err := c.List(context.Background(), key, rev)
		if err != nil {
			return err
		}
		for _, val := range vals {
			fmt.Printf("%s=%s\n", string(val.Key), string(val.Data))
		}
	case "create":
		key := args[1]
		value := args[2]
		return c.Create(context.Background(), key, []byte(value))
	default:
		log.Print("unknown command", args[0])
	}

	return nil
}
