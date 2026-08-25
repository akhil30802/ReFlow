package main

import "testing"

func TestCheckReportPassesWhenEveryPartitionMeetsMinISR(t *testing.T) {
	report := topicReport{
		Partitions: []partitionReport{
			{ID: 0, ISR: []int32{1, 2}},
			{ID: 1, ISR: []int32{1, 2, 3}},
		},
	}

	if violations := checkReport(report, 2); len(violations) != 0 {
		t.Fatalf("expected no violations, got %v", violations)
	}
}

func TestCheckReportFindsUnderReplicatedPartition(t *testing.T) {
	report := topicReport{
		Partitions: []partitionReport{
			{ID: 0, ISR: []int32{1}},
			{ID: 1, ISR: []int32{1, 2}},
		},
	}

	violations := checkReport(report, 2)
	if len(violations) != 1 || violations[0] != "partition=0 isr=1 min_isr=2" {
		t.Fatalf("unexpected violations: %v", violations)
	}
}
