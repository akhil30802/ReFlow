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

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 2 || args[0] != "topic" {
		return fmt.Errorf("usage: reflowctl topic <describe|verify> ...")
	}

	switch args[1] {
	case "describe":
		return describeCommand(args[2:])
	case "verify":
		return verifyCommand(args[2:])
	default:
		return fmt.Errorf("unknown topic command %q", args[1])
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
