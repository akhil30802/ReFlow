package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type partitionReport struct {
	ID       int32   `json:"id"`
	Leader   int32   `json:"leader"`
	Replicas []int32 `json:"replicas"`
	ISR      []int32 `json:"isr"`
}

type topicReport struct {
	Topic      string            `json:"topic"`
	Partitions []partitionReport `json:"partitions"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 6 || args[0] != "topic" || args[1] != "describe" {
		return fmt.Errorf("usage: reflowctl topic describe --bootstrap host:port --topic topic-name")
	}

	fs := flag.NewFlagSet("topic describe", flag.ContinueOnError)
	bootstrap := fs.String("bootstrap", "", "Kafka bootstrap address")
	topic := fs.String("topic", "", "Kafka topic")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	if *bootstrap == "" || *topic == "" {
		return fmt.Errorf("both --bootstrap and --topic are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(*bootstrap),
	)
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
	}
	defer client.Close()

	topicName := *topic
	request := kmsg.NewMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &topicName}}

	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("request topic metadata: %w", err)
	}
	if len(response.Topics) != 1 {
		return fmt.Errorf("Kafka returned no metadata for topic %q", *topic)
	}
	if err := kerr.ErrorForCode(response.Topics[0].ErrorCode); err != nil {
		return fmt.Errorf("topic metadata: %w", err)
	}

	report := topicReport{
		Topic:      *topic,
		Partitions: make([]partitionReport, 0, len(response.Topics[0].Partitions)),
	}
	for _, partition := range response.Topics[0].Partitions {
		if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
			return fmt.Errorf("partition %d metadata: %w", partition.Partition, err)
		}
		report.Partitions = append(report.Partitions, partitionReport{
			ID:       partition.Partition,
			Leader:   partition.Leader,
			Replicas: partition.Replicas,
			ISR:      partition.ISR,
		})
	}
	sort.Slice(report.Partitions, func(i, j int) bool {
		return report.Partitions[i].ID < report.Partitions[j].ID
	})

	output, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	fmt.Println(string(output))
	return nil
}
