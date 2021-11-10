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
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
)

var (
	topic *pubsub.Topic

	// Messages received by this instance.
	messagesMu sync.Mutex
	messages   []string

	// token is used to verify push requests.
	// token = mustGetenv("PUBSUB_VERIFICATION_TOKEN")

	executable             = flag.String("exec", "", "Executable")
	topicName              = flag.String("topic", "", "Topic name")
	ackDeadline            = flag.Int("ackDeadline", 10, "Acknowledgement deadline in seconds")
	gcpProject             = flag.String("gcpProject", os.Getenv("GCP_PROJECT"), "GCP Project ID")
	synchronous            = flag.Bool("synchronous", false, "Enable synchronous mode in subscriber receive settings")
	maxOutstandingMessages = flag.Int("maxOutstandingMessages", 0, "Set MaxOutstandingMessages in subscriber receive settings")
	verbose                = flag.Bool("verbose", false, "Verbose")
)

func init() {
	flag.Parse()

	formatter := &logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyMsg:   "message",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyTime:  "timestamp",
		},
	}

	if *verbose {
		logrus.SetLevel(logrus.DebugLevel)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}
	logrus.SetFormatter(formatter)

	// Output to stdout instead of the default stderr
	logrus.SetOutput(os.Stdout)
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	logrus.Infof("%s took %s", name, elapsed)
}

func main() {
	cctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-c
		logrus.Infof("Caught signal: %v", sig)
		cancel()
	}()

	client, err := pubsub.NewClient(cctx, *gcpProject)
	if err != nil {
		logrus.Fatal(err)
	}
	defer func() {
		client.Close()
		logrus.Info("Closing connection")
	}()

	logrus.Info("Connected")

	topic = client.Topic(*topicName)
	exists, err := topic.Exists(cctx)
	if err != nil {
		logrus.Fatal(err)
	}
	if !exists {
		logrus.Fatalf("Topic %s doesn't exist", *topicName)
	}

	subscriptionName := *topicName + "-consumer"
	sub := client.Subscription(subscriptionName)
	exists, err = sub.Exists(cctx)
	if err != nil {
		logrus.Fatal(err)
	}
	// Create the subscription if it doesn't exist
	if !exists {
		logrus.Infof("Creating subscription %s", subscriptionName)
		sub, err = client.CreateSubscription(cctx, subscriptionName, pubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: time.Duration(*ackDeadline) * time.Second,
		})
		if err != nil {
			logrus.Fatal(err)
		}
		logrus.Info("Created subscription")
	} else {
		logrus.Infof("Subscription %s already exists", subscriptionName)
	}

	// https://pkg.go.dev/cloud.google.com/go/pubsub#ReceiveSettings
	sub.ReceiveSettings.Synchronous = *synchronous
	sub.ReceiveSettings.MaxOutstandingMessages = *maxOutstandingMessages

	var mu sync.Mutex

	logrus.Info("Waiting for messages")
	// Receive blocks until the context is cancelled or an error occurs.
	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()

		if *verbose {
			logrus.WithFields(logrus.Fields{
				"message_id":        msg.ID,
				"publish_time_unix": msg.PublishTime.Unix(),
				"publish_time":      msg.PublishTime.Format("2006-01-02 15:04:05"),
			}).Info("Received message")
			logrus.WithFields(logrus.Fields{
				"message_id":        msg.ID,
				"publish_time_unix": msg.PublishTime.Unix(),
				"publish_time":      msg.PublishTime.Format("2006-01-02 15:04:05"),
			}).Info(string(msg.Data))
		}
		command := strings.Split(*executable, " ")

		cmd := exec.Command(command[0], command[1:]...)
		cmd.Stdin = strings.NewReader(string(msg.Data))
		if *verbose {
			logrus.WithFields(logrus.Fields{
				"message_id":        msg.ID,
				"publish_time_unix": msg.PublishTime.Unix(),
				"publish_time":      msg.PublishTime.Format("2006-01-02 15:04:05"),
			}).Info("Running command")
		}
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		start := time.Now()
		cmd.Run()
		elapsed := time.Since(start)

		if *verbose {
			logrus.WithFields(logrus.Fields{
				"message_id":        msg.ID,
				"publish_time_unix": msg.PublishTime.Unix(),
				"publish_time":      msg.PublishTime.Format("2006-01-02 15:04:05"),
				"command_duration":  elapsed.Seconds(),
			}).Info("Ran command")
		}

		if *verbose {
			logrus.WithFields(logrus.Fields{
				"message_id":        msg.ID,
				"publish_time_unix": msg.PublishTime.Unix(),
				"publish_time":      msg.PublishTime.Format("2006-01-02 15:04:05"),
			}).Info("Acking message")
		}
		msg.Ack()
		if *verbose {
			logrus.WithFields(logrus.Fields{
				"message_id":        msg.ID,
				"publish_time_unix": msg.PublishTime.Unix(),
				"publish_time":      msg.PublishTime.Format("2006-01-02 15:04:05"),
			}).Info("Acked message")
		}
	})
	if err != nil {
		logrus.Fatal(err)
	}
}

func mustGetenv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		logrus.Fatalf("%s environment variable not set.", k)
	}
	return v
}
