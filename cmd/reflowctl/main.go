package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
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

type topicFlags struct {
	bootstrap string
	topic     string
	minISR    int
}

type produceFlags struct {
	bootstrap string
	topic     string
	count     int
	keyPrefix string
}

type orderEvent struct {
	EventID      string    `json:"event_id"`
	EventType    string    `json:"event_type"`
	EventVersion int       `json:"event_version"`
	OccurredAt   time.Time `json:"occurred_at"`
	Payload      struct {
		OrderID  string `json:"order_id"`
		Status   string `json:"status"`
		Sequence int    `json:"sequence"`
	} `json:"payload"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: reflowctl <topic|event> <command> ...")
	}

	switch args[0] {
	case "topic":
		switch args[1] {
		case "describe":
			return describeCommand(args[2:])
		case "verify":
			return verifyCommand(args[2:])
		default:
			return fmt.Errorf("unknown topic command %q", args[1])
		}
	case "event":
		if args[1] == "produce" {
			return produceCommand(args[2:])
		}
		return fmt.Errorf("unknown event command %q", args[1])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func describeCommand(args []string) error {
	options, err := parseTopicFlags(args, "topic describe")
	if err != nil {
		return err
	}

	report, err := fetchTopicReport(options.bootstrap, options.topic)
	if err != nil {
		return err
	}

	output, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	fmt.Println(string(output))
	return nil
}

func verifyCommand(args []string) error {
	options, err := parseTopicFlags(args, "topic verify")
	if err != nil {
		return err
	}
	if options.minISR < 1 {
		return fmt.Errorf("--min-isr must be at least 1")
	}

	report, err := fetchTopicReport(options.bootstrap, options.topic)
	if err != nil {
		return err
	}

	violations := checkReport(report, options.minISR)
	if len(violations) > 0 {
		return fmt.Errorf("UNHEALTHY topic=%s %s", options.topic, strings.Join(violations, "; "))
	}

	fmt.Printf("OK topic=%s partitions=%d min_isr=%d\n", options.topic, len(report.Partitions), options.minISR)
	return nil
}

func parseTopicFlags(args []string, command string) (topicFlags, error) {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	bootstrap := fs.String("bootstrap", "", "Kafka bootstrap address")
	topic := fs.String("topic", "", "Kafka topic")
	minISR := fs.Int("min-isr", 1, "minimum in-sync replicas")
	if err := fs.Parse(args); err != nil {
		return topicFlags{}, err
	}
	if fs.NArg() != 0 {
		return topicFlags{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *bootstrap == "" || *topic == "" {
		return topicFlags{}, fmt.Errorf("both --bootstrap and --topic are required")
	}

	return topicFlags{
		bootstrap: *bootstrap,
		topic:     *topic,
		minISR:    *minISR,
	}, nil
}

func parseProduceFlags(args []string) (produceFlags, error) {
	fs := flag.NewFlagSet("event produce", flag.ContinueOnError)
	bootstrap := fs.String("bootstrap", "", "Kafka bootstrap address")
	topic := fs.String("topic", "", "Kafka topic")
	count := fs.Int("count", 1, "number of events to produce")
	keyPrefix := fs.String("key-prefix", "order", "prefix used to build event keys")
	if err := fs.Parse(args); err != nil {
		return produceFlags{}, err
	}
	if fs.NArg() != 0 {
		return produceFlags{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *bootstrap == "" || *topic == "" {
		return produceFlags{}, fmt.Errorf("both --bootstrap and --topic are required")
	}
	if *keyPrefix == "" {
		return produceFlags{}, fmt.Errorf("--key-prefix must not be empty")
	}

	return produceFlags{
		bootstrap: *bootstrap,
		topic:     *topic,
		count:     *count,
		keyPrefix: *keyPrefix,
	}, nil
}

func produceCommand(args []string) error {
	options, err := parseProduceFlags(args)
	if err != nil {
		return err
	}
	if options.count < 1 {
		return fmt.Errorf("--count must be at least 1")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(options.bootstrap),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(15*time.Second),
		kgo.ProduceRequestTimeout(5*time.Second),
		kgo.ClientID("reflowctl-producer"),
	)
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
	}
	defer client.Close()

	records := make([]*kgo.Record, 0, options.count)
	for i := 0; i < options.count; i++ {
		key := fmt.Sprintf("%s-%d", options.keyPrefix, i%10)
		event := orderEvent{
			EventID:      fmt.Sprintf("%s-%d-%d", key, time.Now().UnixNano(), i),
			EventType:    "order.status_changed",
			EventVersion: 1,
			OccurredAt:   time.Now().UTC(),
		}
		event.Payload.OrderID = key
		event.Payload.Status = "created"
		event.Payload.Sequence = i / 10

		value, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encode event %d: %w", i, err)
		}
		records = append(records, &kgo.Record{
			Topic: options.topic,
			Key:   []byte(key),
			Value: value,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("produce %d events: %w", options.count, err)
	}

	partitions := make(map[int32]int)
	for _, record := range records {
		partitions[record.Partition]++
	}
	fmt.Printf("PRODUCED topic=%s count=%d partitions=%v\n", options.topic, len(records), partitions)
	return nil
}

func fetchTopicReport(bootstrap, topic string) (topicReport, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrap))
	if err != nil {
		return topicReport{}, fmt.Errorf("create Kafka client: %w", err)
	}
	defer client.Close()

	topicName := topic
	request := kmsg.NewMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &topicName}}

	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return topicReport{}, fmt.Errorf("request topic metadata: %w", err)
	}
	if len(response.Topics) != 1 {
		return topicReport{}, fmt.Errorf("Kafka returned no metadata for topic %q", topic)
	}
	if err := kerr.ErrorForCode(response.Topics[0].ErrorCode); err != nil {
		return topicReport{}, fmt.Errorf("topic metadata: %w", err)
	}

	report := topicReport{
		Topic:      topic,
		Partitions: make([]partitionReport, 0, len(response.Topics[0].Partitions)),
	}
	for _, partition := range response.Topics[0].Partitions {
		if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
			return topicReport{}, fmt.Errorf("partition %d metadata: %w", partition.Partition, err)
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

	return report, nil
}

func checkReport(report topicReport, minISR int) []string {
	violations := make([]string, 0)
	for _, partition := range report.Partitions {
		if len(partition.ISR) < minISR {
			violations = append(violations, fmt.Sprintf(
				"partition=%d isr=%d min_isr=%d",
				partition.ID,
				len(partition.ISR),
				minISR,
			))
		}
	}
	return violations
}
