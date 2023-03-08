package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
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
	Offset    int
	Prefix    string
	RBack     bool
}

func cfginit() {
	flag.StringVar(&config.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&config.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&config.DBName, "db", "defaultdb", "Name of the database to use")
	flag.IntVar(&config.BatchSize, "batchsize", 1000, "Batch size")
	flag.IntVar(&config.Offset, "offset", 0, "Initial counter value")
	flag.StringVar(&config.Prefix, "prefix", "LINE", "Prefix for Key generation")
	flag.BoolVar(&config.RBack, "readback", false, "Don't write, read back instead (and check value)")
	flag.Parse()
}

func connect() (client immuclient.ImmuClient, ctx context.Context) {
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)

	client = immuclient.NewClient().WithOptions(opts)
	ctx = context.Background()
	err := client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to use the database. Reason:", err)
	}
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
			Key:   []byte(fmt.Sprintf("%s%.9d", config.Prefix, idx+config.Offset)),
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

func checker(ch chan string, out chan bool) {
	client, ctx := connect()
	keys := make([][]byte, config.BatchSize)
	vals := make([][]byte, config.BatchSize)
	var idx = 0
	var cnt = 0
	t0 := time.Now()
	for line := range ch {
		keys[cnt] = []byte(fmt.Sprintf("%s%.9d", config.Prefix, idx+config.Offset))
		vals[cnt] = []byte(line)
		idx++
		cnt++
		if cnt == config.BatchSize {
			readback, err := client.GetAll(ctx, keys)
			if err != nil {
				log.Fatalln("Failed to read the batch. Reason:", err)
			} else {
				log.Printf("Read %d lines", idx)
			}
			for j := 0; j < cnt; j++ {
				if bytes.Compare(vals[j], readback.Entries[j].Value) != 0 {
					log.Fatal("Mismatch %s <> %s", string(vals[j]), string(readback.Entries[j].Value))
				}
			}
			cnt = 0
		}

	}
	if cnt > 0 {
		keys = keys[:cnt]
		readback, err := client.GetAll(ctx, keys)
		if err != nil {
			log.Fatalln("Failed to read the batch. Reason:", err)
		} else {
			log.Printf("Read %d lines", idx)
		}
		for j := 0; j < cnt; j++ {
			if bytes.Compare(vals[j], readback.Entries[j].Value) != 0 {
				log.Fatal("Mismatch %s <> %s", string(vals[j]), string(readback.Entries[j].Value))
			}
		}
	}
	log.Printf("DONE: read %d lines in %s", idx, time.Now().Sub(t0))
	out <- true
}

func main() {
	cfginit()
	ch := make(chan string)
	out := make(chan bool)
	if config.RBack {
		go checker(ch, out)
	} else {
		go inserter(ch, out)
	}
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
