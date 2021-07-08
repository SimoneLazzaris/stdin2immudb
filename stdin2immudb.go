package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc/metadata"
	"log"
	"os"
	"time"
)

var config struct {
	IpAddr    string
	Port      int
	Username  string
	Password  string
	DBName    string
	BatchSize int
}

func cfginit() {
	flag.StringVar(&config.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&config.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&config.DBName, "db", "defaultdb", "Name of the database to use")
	flag.IntVar(&config.BatchSize, "batchsize", 1000, "Batch size")
	flag.Parse()
}

func connect() (client immuclient.ImmuClient, ctx context.Context) {
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)

	client, err := immuclient.NewImmuClient(opts)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err)
	}

	ctx = context.Background()

	login, err := client.Login(ctx, []byte(config.Username), []byte(config.Password))
	if err != nil {
		log.Fatalln("Failed to login. Reason:", err.Error())
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", login.GetToken()))

	udr, err := client.UseDatabase(ctx, &schema.Database{DatabaseName: config.DBName})
	if err != nil {
		log.Fatalln("Failed to use the database. Reason:", err)
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", udr.GetToken()))
	return
}

func inserter(ch chan string, out chan bool) {
	client, ctx := connect()
	kvs := make([]*schema.KeyValue, config.BatchSize)
	var idx = 0
	var cnt = 0
	t0 := time.Now()
	for line := range ch {
		kvs[cnt] = &schema.KeyValue{
			Key:   []byte(fmt.Sprintf("LINE%.9d", idx)),
			Value: []byte(line),
		}
		idx++
		cnt++
		if cnt == config.BatchSize {
			kvList := &schema.SetRequest{KVs: kvs}
			if _, err := client.SetAll(ctx, kvList); err != nil {
				log.Fatalln("Failed to submit the batch. Reason:", err)
			} else {
				log.Printf("Inserted %d lines", idx)
			}
			cnt = 0
		}
	}
	if cnt > 0 {
		kvs = kvs[:cnt]
		kvList := &schema.SetRequest{KVs: kvs}
		if _, err := client.SetAll(ctx, kvList); err != nil {
			log.Fatalln("Failed to submit the batch. Reason:", err)
		} else {
			log.Printf("Inserted %d lines", idx)
		}
	}
	log.Printf("DONE: inserted %d lines in %s", idx, time.Now().Sub(t0))
	out <- true
}

func main() {
	cfginit()
	ch := make(chan string)
	out := make(chan bool)
	go inserter(ch, out)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		ch <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		log.Println(err)
	}
	close(ch)
	<-out
}
