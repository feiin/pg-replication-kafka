package main

import (
	"context"
	"testing"
)

func TestSaveReplicateState(t *testing.T) {
	r := NewReplicator("./test.state", "test_db", "", "test_pubname", "test_slotname", "test_kafkatopic")

	err := r.SyncReplicateStatus(context.Background(), &ReplicationStatus{LastWriteLSN: 999})
	if err != nil {
		t.Error("SyncReplicateStatus error")
	}

	readLSN := r.GetLastReplicateStatus(context.Background())
	if readLSN != 999 {
		t.Errorf("save replicate state failed")
	}
}

func TestNotFoundReplicateState(t *testing.T) {
	r := NewReplicator("./not_found.state", "test_db", "", "test_pubname", "test_slotname", "test_kafkatopic")

	readLSN := r.GetLastReplicateStatus(context.Background())
	if readLSN != 0 {
		t.Errorf("TestNotFoundReplicateState failed")
	}
}
