package main

import (
	"context"
	"errors"
	"fmt"
	"pg-replication-kafka/logger"
	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

const outputPlugin = "pgoutput"

type Replicator struct {
	stateFilePath   string
	dsn             string
	publicationName string
	slotName        string
	running         bool
	stop            chan struct{}
	mu              sync.RWMutex
}

func NewReplicateDSN(database string, user string, password string, host string, port string) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable replication=database", host, port, user, password, database)
}

func NewReplicator(stateFilePath string, dsn string, slotName string, publicationName string) *Replicator {
	return &Replicator{
		stateFilePath:   stateFilePath,
		dsn:             dsn,
		publicationName: publicationName,
	}
}

func (r *Replicator) sendStandbyStatusUpdate(ctx context.Context, conn *pgconn.PgConn, lastWriteLSN pglogrepl.LSN, lastFlushLSN pglogrepl.LSN, lastApplyLSN pglogrepl.LSN) error {
	// https://www.postgresql.org/docs/current/protocol-replication.html
	err := pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lastWriteLSN + 1,
		WALFlushPosition: lastFlushLSN + 1,
		WALApplyPosition: lastApplyLSN + 1,
	})
	if err != nil {
		logger.ErrorWith(ctx, err).Uint64("lastWriteLSN", uint64(lastWriteLSN)).Uint64("lastFlushLSN", uint64(lastFlushLSN)).Uint64("lastApplyLSN", uint64(lastApplyLSN)).Msg("sendStandbyStatusUpdate error")
		return err
	}

	logger.Info(ctx).Uint64("lastWriteLSN", uint64(lastWriteLSN)).Uint64("lastFlushLSN", uint64(lastFlushLSN)).Uint64("lastApplyLSN", uint64(lastApplyLSN)).Msg("sendStandbyStatusUpdate success")
	return nil
}

func (r *Replicator) checkPublicationExists(ctx context.Context, conn *pgconn.PgConn) bool {

	results, err := conn.Exec(ctx, fmt.Sprintf(`SELECT * FROM pg_publication WHERE pubname = '%s'`, r.publicationName)).ReadAll()
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("checkPublicationExists error")
		return false
	}
	result := results[0]
	logger.Info(ctx).Interface("result", result).Msg("checkPublicationExists")
	if len(result.Rows) == 0 {
		return false
	}
	return true
}

func (r *Replicator) createReplicationSlotIfNeed(ctx context.Context, conn *pgconn.PgConn) error {
	results, err := conn.Exec(ctx, fmt.Sprintf(`select * from pg_replication_slots where slot_name='%s'`, r.slotName)).ReadAll()
	if err != nil {
		return err
	}
	result := results[0]
	existSlot := false
	if len(result.Rows) > 0 {
		existSlot = true
	}

	if !existSlot {
		_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{})
		if err != nil {
			err, ok := err.(*pgconn.PgError)
			if ok && err.Code == "42710" {
				return nil
			}
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func (r *Replicator) BeginReplication(ctx context.Context) error {
	conn, err := pgconn.Connect(context.Background(), r.dsn)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("BeginReplication error")
	}

	defer func() {
		err := conn.Close(ctx)
		if err != nil {
			logger.ErrorWith(ctx, err).Msg("BeginReplication close db error")
		}
	}()

	// check pubname if exists
	existPubname := r.checkPublicationExists(ctx, conn)
	if !existPubname {
		return errors.New("publication not exists")
	}

	// make sure slot_name created
	err = r.createReplicationSlotIfNeed(ctx, conn)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("createReplicationSlotIfNeed error")
		return err
	}

	return nil
}
