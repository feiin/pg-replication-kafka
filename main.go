package main

import (
	"context"
	"flag"
	"pg-replication-kafka/logger"
)

var (
	host, port, user, password, dbName, publicationName, slotName string
)

func main() {
	flag.StringVar(&host, "host", "127.0.0.1", "postgres host")
	flag.StringVar(&port, "port", "5432", "postgres port")
	flag.StringVar(&user, "user", "postgres", "postgres user")
	flag.StringVar(&password, "password", "", "postgres password")
	flag.StringVar(&dbName, "db", "postgres", "postgres database name")
	flag.StringVar(&publicationName, "pubname", "", "publication name created via CREATE PUBLICATION {name} FOR ALL TABLES")
	flag.StringVar(&slotName, "slotName", "pg_replicate_kafka", "slot name")
	flag.Parse()

	logicReplicator := NewReplicator("state.json", dbName, NewReplicateDSN(dbName, user, password, host, port), slotName, publicationName)

	ctx := context.Background()
	err := logicReplicator.BeginReplication(ctx)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("BeginReplication error")
	}

}
