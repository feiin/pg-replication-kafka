package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"pg-replication-kafka/logger"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const outputPlugin = "pgoutput"

var typeMap = pgtype.NewMap()

type Replicator struct {
	stateFilePath   string
	dsn             string
	publicationName string
	slotName        string
	running         bool
	stop            chan struct{}
	done            chan struct{}
	mu              sync.RWMutex
	err             error
	db              string
	kafkaTopic      string
}

type ReplicationStatus struct {
	LastWriteLSN    pglogrepl.LSN
	LastReceivedLSN pglogrepl.LSN
}

type ReplicatePosition struct {
	ReplicationStatus
	UpdateStandbyStatus bool
	Relations           map[uint32]*pglogrepl.RelationMessageV2

	Rows       []*RowData
	inStream   bool
	CommitTime time.Time
	Xid        uint32
}

func NewReplicateDSN(database string, user string, password string, host string, port string) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable replication=database", host, port, user, password, database)
}

func NewReplicator(stateFilePath string, db string, dsn string, slotName string, publicationName string, kafkaTopic string) *Replicator {
	return &Replicator{
		stateFilePath:   stateFilePath,
		dsn:             dsn,
		publicationName: publicationName,
		done:            make(chan struct{}),
		db:              db,
		kafkaTopic:      kafkaTopic,
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
	logger.Info(ctx).Interface("result", len(result.Rows) == 0).Msg("checkPublicationExists")
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

func (r *Replicator) IsRunning() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.running
}

func (r *Replicator) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return
	}

	close(r.stop)
}

func (r *Replicator) SyncReplicateStatus(ctx context.Context, status *ReplicationStatus) error {
	return os.WriteFile(r.stateFilePath, []byte(fmt.Sprintf("%d", status.LastWriteLSN)), 0644)
}

func (r *Replicator) GetLastReplicateStatus(ctx context.Context) pglogrepl.LSN {
	_, err := os.Stat(r.stateFilePath)
	if err != nil && os.IsNotExist(err) {
		return 0
	}

	if err != nil {
		logger.ErrorWith(ctx, err).Msg("GetLastReplicateStatus error")
		return 0
	}

	// read file

	data, err := os.ReadFile(r.stateFilePath)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("GetLastReplicateStatus ReadFile error")
		return 0
	}

	stringLSN := string(data)
	// convert to LSN
	lsn, err := strconv.ParseUint(stringLSN, 10, 64)

	if err != nil {
		logger.ErrorWith(ctx, err).Msg("GetLastReplicateStatus ParseUint error")
		return 0
	}

	return pglogrepl.LSN(lsn)

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

	startLSN := 0
	beginLSN := pglogrepl.LSN(startLSN)

	// load from state file
	stateLSN := r.GetLastReplicateStatus(ctx)
	if stateLSN > beginLSN {
		beginLSN = stateLSN
	}

	pluginArgs := []string{
		"proto_version '2'",
		"publication_names '" + r.publicationName + "'",
		"messages 'true'",
		"streaming 'true'",
	}

	err = pglogrepl.StartReplication(ctx, conn, slotName, beginLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
		Mode:       pglogrepl.LogicalReplication,
	})
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("StartReplication error")
		return err
	}

	r.mu.Lock()
	r.running = true
	r.stop = make(chan struct{})
	r.mu.Unlock()

	logger.Info(ctx).Msg("begin replication")

	replicatePos := ReplicatePosition{
		ReplicationStatus: ReplicationStatus{
			LastWriteLSN: beginLSN,
		},
		Relations: map[uint32]*pglogrepl.RelationMessageV2{},
	}

	go func() {
		defer func() {
			r.done <- struct{}{}
		}()

		for {

			receiveCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

			var rawMsg pgproto3.BackendMessage
			select {
			case <-r.stop:
				logger.Info(ctx).Msg("replication stoped")
				return
			case <-ctx.Done():
				cancel()
				return
			default:
				msg, err := conn.ReceiveMessage(receiveCtx)
				cancel()
				if pgconn.Timeout(err) || err == context.DeadlineExceeded {
					continue
				} else if err != nil {
					logger.ErrorWith(ctx, err).Msg("ReceiveMessage error")
					r.err = err
					break
				}
				rawMsg = msg
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				err := fmt.Errorf("Received Postgres WAL error: %+v", errMsg)
				r.err = err
				break
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				err := fmt.Errorf("Received unexpected message: %+v", rawMsg)
				r.err = err
				break

			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					err := fmt.Errorf("ParsePrimaryKeepaliveMessage error: %+v", err)
					r.err = err
					return
				}

				if pkm.ServerWALEnd > replicatePos.LastWriteLSN {
					replicatePos.LastWriteLSN = pkm.ServerWALEnd
				}
				if pkm.ReplyRequested {
					replicatePos.UpdateStandbyStatus = true
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					r.err = fmt.Errorf("ParseXLogData failed: %w", err)
					return
				}

				commit, err := r.processMessage(ctx, xld, &replicatePos)
				if err != nil {
					r.err = fmt.Errorf("ParseXLogData failed: %w", err)
					return
				}

				if commit {
					// TODO
					for _, row := range replicatePos.Rows {
						encodeData, err := json.Marshal(row)
						if err != nil {
							logger.ErrorWith(ctx, err).Msg("json.Marshal row error")
							r.err = err
							return
						}
						err = sendKafkaMsg(ctx, encodeData, r.kafkaTopic)
						if err != nil {
							logger.ErrorWith(ctx, err).Str("kafkaTopic", r.kafkaTopic).Msg("sendKafkaMsg row error")
							r.err = err
							return
						}
					}
					replicatePos.UpdateStandbyStatus = true
					logger.Info(ctx).Interface("LastWriteLSN", replicatePos.LastWriteLSN).Msg("commit local")
					replicatePos.Rows = nil
				}

			}
			if replicatePos.UpdateStandbyStatus {
				r.sendStandbyStatusUpdate(ctx, conn, replicatePos.LastWriteLSN, replicatePos.LastWriteLSN, replicatePos.LastReceivedLSN)
				replicatePos.UpdateStandbyStatus = false
			}

		}

	}()

	<-r.done
	if err != nil {
		logger.ErrorWith(ctx, r.err).Msg("replication done with error")
		return r.err
	}
	logger.Info(ctx).Msg("replication done")
	return nil
}

func (r *Replicator) processMessage(ctx context.Context, xld pglogrepl.XLogData, replicatePosition *ReplicatePosition) (bool, error) {
	logicalMsg, err := pglogrepl.ParseV2(xld.WALData, replicatePosition.inStream)
	if err != nil {
		return false, fmt.Errorf("processMessage ParseV2: %+v", err)
	}

	replicatePosition.LastReceivedLSN = xld.ServerWALEnd
	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		replicatePosition.Relations[logicalMsg.RelationID] = logicalMsg
	case *pglogrepl.BeginMessage:
		// START TRANSACTION
		if replicatePosition.LastWriteLSN < logicalMsg.FinalLSN {
			replicatePosition.LastWriteLSN = logicalMsg.FinalLSN
		}
		replicatePosition.CommitTime = logicalMsg.CommitTime
		replicatePosition.Xid = logicalMsg.Xid
		logger.Info(ctx).Uint64("FinalLSN", uint64(logicalMsg.FinalLSN)).Msg("processMessage START TRANSACTION")

	case *pglogrepl.CommitMessage:

		// COMMIT
		logger.Info(ctx).Uint64("CommitLSN", uint64(logicalMsg.CommitLSN)).Uint64("TransactionEndLSN", uint64(logicalMsg.TransactionEndLSN)).Msg("processMessage COMMIT")

		return true, nil

	case *pglogrepl.InsertMessageV2:
		rel, ok := replicatePosition.Relations[logicalMsg.RelationID]
		if !ok {
			err := fmt.Errorf("insert action unknown relation id %d", logicalMsg.RelationID)
			return false, err
		}

		rowData := RowData{}
		rowData.Action = "insert"
		rowData.Gtid = fmt.Sprintf("%d", replicatePosition.Xid)
		rowData.LogPos = uint64(replicatePosition.LastWriteLSN)
		rowData.Schema = r.db
		rowData.Namespace = rel.Namespace
		rowData.Table = rel.RelationName
		rowData.Timestamp = uint32(replicatePosition.CommitTime.Unix())
		insertValue, err := getMapValues(logicalMsg.Tuple.Columns, rel)
		if err != nil {
			logger.ErrorWith(ctx, err).Str("namespace", rel.Namespace).Str("RelationName", rel.RelationName).Msg("InsertMessageV2 error")
		}
		rowData.Values = insertValue

		replicatePosition.Rows = append(replicatePosition.Rows, &rowData)

	case *pglogrepl.UpdateMessageV2:
		rel, ok := replicatePosition.Relations[logicalMsg.RelationID]
		if !ok {
			err := fmt.Errorf("update action unknown relation id %d", logicalMsg.RelationID)
			return false, err
		}

		var oldValues map[string]any
		if logicalMsg.OldTuple != nil {
			oldValues, err = getMapValues(logicalMsg.OldTuple.Columns, rel)
			if err != nil {
				return false, err
			}
		}
		newValues, err := getMapValues(logicalMsg.NewTuple.Columns, rel)
		if err != nil {
			return false, err
		}

		rowData := RowData{}
		rowData.Action = "update"
		rowData.Gtid = fmt.Sprintf("%d", replicatePosition.Xid)
		rowData.LogPos = uint64(replicatePosition.LastWriteLSN)
		rowData.Schema = r.db
		rowData.Namespace = rel.Namespace
		rowData.Table = rel.RelationName
		rowData.BeforeValues = oldValues
		rowData.AfterValues = newValues
		rowData.Timestamp = uint32(replicatePosition.CommitTime.Unix())

		replicatePosition.Rows = append(replicatePosition.Rows, &rowData)

	case *pglogrepl.DeleteMessageV2:
		rel, ok := replicatePosition.Relations[logicalMsg.RelationID]
		if !ok {
			err := fmt.Errorf("delete action unknown relation id %d", logicalMsg.RelationID)
			return false, err
		}

		rowData := RowData{}
		rowData.Action = "delete"
		rowData.Gtid = fmt.Sprintf("%d", replicatePosition.Xid)
		rowData.LogPos = uint64(replicatePosition.LastWriteLSN)
		rowData.Schema = r.db
		rowData.Namespace = rel.Namespace
		rowData.Table = rel.RelationName
		rowData.Timestamp = uint32(replicatePosition.CommitTime.Unix())
		insertValue, err := getMapValues(logicalMsg.OldTuple.Columns, rel)
		if err != nil {
			logger.ErrorWith(ctx, err).Str("namespace", rel.Namespace).Str("RelationName", rel.RelationName).Msg("insertValue error")
		}

		rowData.Values = insertValue
		replicatePosition.Rows = append(replicatePosition.Rows, &rowData)

	case *pglogrepl.TruncateMessageV2:
		logger.Info(ctx).Uint32("xid", logicalMsg.Xid).Msg("truncate message")

	case *pglogrepl.TypeMessageV2:
		logger.Info(ctx).Uint32("xid", logicalMsg.Xid).Msg("type message")

	case *pglogrepl.OriginMessage:
		logger.Info(ctx).Str("xid", logicalMsg.Name).Msg("origin message")

	case *pglogrepl.LogicalDecodingMessageV2:
		logger.Info(ctx).Uint32("xid", logicalMsg.Xid).Msg("logical decoding message")

	case *pglogrepl.StreamStartMessageV2:
		replicatePosition.inStream = true
		logger.Info(ctx).Uint32("xid", logicalMsg.Xid).Msg("stream start message")

	case *pglogrepl.StreamStopMessageV2:
		replicatePosition.inStream = false
		logger.Info(ctx).Msg("stream stop message")

	case *pglogrepl.StreamCommitMessageV2:
		logger.Info(ctx).Uint32("xid", logicalMsg.Xid).Msg("stream commit message")

		log.Printf("Stream commit message: xid %d\n", logicalMsg.Xid)
	case *pglogrepl.StreamAbortMessageV2:
		logger.Info(ctx).Uint32("xid", logicalMsg.Xid).Msg("stream abort message")

	default:
		logger.Warn(ctx).Interface("logicalMsg", logicalMsg).Msg("unknown message type")
	}

	return false, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func getMapValues(cols []*pglogrepl.TupleDataColumn, rel *pglogrepl.RelationMessageV2) (map[string]any, error) {
	values := map[string]any{}
	for idx, col := range cols {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
		case 't': //text
			val, err := decodeTextColumnData(col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, fmt.Errorf("error decoding column data: %w", err)
			}
			values[colName] = val
		}
	}
	return values, nil
}
