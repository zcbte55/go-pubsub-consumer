// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Sample pubsub demonstrates use of the cloud.google.com/go/pubsub package from App Engine flexible environment.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"

	"cloud.google.com/go/pubsub"
)

var (
	topic *pubsub.Topic

	// Messages received by this instance.
	messagesMu sync.Mutex
	messages   []string

	// token is used to verify push requests.
	// token = mustGetenv("PUBSUB_VERIFICATION_TOKEN")

	executable = flag.String("exec", "", "Executable")
	topicName  = flag.String("topic", "", "Topic name")
	verbose    = flag.Bool("verbose", false, "Verbose")
)

const maxMessages = 10

func init() {
	flag.Parse()
}

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	go func() {
		<-c
		cancel()
	}()

	client, err := pubsub.NewClient(ctx, "nbcu-res-aieng-dev-reporting")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected")

	topic = client.Topic(*topicName)

	fmt.Println("Got topic")
	// Create the topic if it doesn't exist.
	exists, err := topic.Exists(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if !exists {
		log.Printf("Topic %v doesn't exist - creating it", *topicName)
		_, err = client.CreateTopic(ctx, *topicName)
		if err != nil {
			log.Fatal(err)
		}
	}

	subscriptionName := *topicName + "-consumer"
	sub := client.Subscription(subscriptionName)

	if exists, _ := sub.Exists(ctx); !exists {
		sub, err = client.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{
			Topic: topic,
		})

		if err != nil {
			log.Fatal(err)
		}
	}

	// Create a channel to handle messages to as they come in.
	cm := make(chan *pubsub.Message)
	// Handle individual messages in a goroutine.
	go func() {
		for {
			select {
			case msg := <-cm:
				if *verbose {
					fmt.Println(string(msg.Data))
				}
				command := strings.Split(*executable, " ")

				cmd := exec.Command(command[0], command[1:len(command)]...)
				cmd.Stdin = strings.NewReader(string(msg.Data))
				if *verbose {
					fmt.Println("Running command")
				}
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				cmd.Run()

				if *verbose {
					fmt.Println("Ran command")
				}

				if *verbose {
					fmt.Println("Ack'ing message")
				}
				msg.Ack()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Receive blocks until the context is cancelled or an error occurs.
	fmt.Println("Listening")

	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		cm <- msg
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	close(cm)
}

func mustGetenv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s environment variable not set.", k)
	}
	return v
}
