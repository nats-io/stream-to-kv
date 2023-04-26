package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/nats-io/nats.go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func run() error {
	var (
		natsURL string
	)

	flag.StringVar(&natsURL, "nats.url", nats.DefaultURL, "NATS URL")

	flag.Parse()

	args := flag.Args()

	if len(args) != 2 {
		return fmt.Errorf("Usage: stream-to-kv [-s server (%s)] <stream> <kv>", nats.DefaultURL)
	}

	streamName := args[0]
	bucketName := args[1]

	nc, err := nats.Connect(natsURL)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	kvi, err := js.StreamInfo(fmt.Sprintf("KV_%s", bucketName))
	if err != nil {
		return err
	}

	kv, err := js.KeyValue(bucketName)
	if err != nil {
		return err
	}

	opts := []nats.SubOpt{
		nats.BindStream(streamName),
		nats.OrderedConsumer(),
	}

	if kvi.State.LastSeq > 0 {
		opts = append(opts, nats.StartSequence(kvi.State.LastSeq+1))
	}

	handler := func(msg *nats.Msg) {
		_, err := kv.Put(msg.Subject, msg.Data)
		if err != nil {
			log.Printf("Error updating key %s: %s", msg.Subject, err)
		}
	}

	sub, err := js.Subscribe("", handler, opts...)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)
	<-sigch

	return nil
}
