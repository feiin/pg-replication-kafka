package main

import (
	"context"
	"flag"
	"fmt"
	"pg-replication-kafka/logger"
	"strings"
)

var (
	port                                                    int
	host, user, password, dbName, publicationName, slotName string
	kafkaTopicName, kafkaAddr                               string
)

func main() {
	flag.StringVar(&kafkaTopicName, "kafka_topic_name", "", "Kafka topic name")
	flag.StringVar(&kafkaAddr, "kafka_addr", "", "Kafka address")
	flag.StringVar(&host, "host", "127.0.0.1", "postgres host")
	flag.IntVar(&port, "port", 5432, "postgres port")
	flag.StringVar(&user, "user", "postgres", "postgres user")
	flag.StringVar(&password, "password", "", "postgres password")
	flag.StringVar(&dbName, "db", "postgres", "postgres database name")
	flag.StringVar(&publicationName, "pubname", "", "publication name created via CREATE PUBLICATION {name} FOR ALL TABLES")
	flag.StringVar(&slotName, "slot_name", "pg_replicate_kafka", "slot name")
	flag.Parse()

	defaultStateFile := fmt.Sprintf("pg_replication_%s.state", dbName)
	logicReplicator := NewReplicator(defaultStateFile, dbName, NewReplicateDSN(dbName, user, password, host, port), slotName, publicationName, kafkaTopicName)

	ctx := context.Background()

	kafkaAddress := strings.Split(kafkaAddr, ",")

	err := InitKafka(kafkaAddress)
	if err != nil {
		logger.ErrorWith(context.Background(), err).Msg("InitKafka error")
		panic(err)
	}
	logger.Info(ctx).Msg("connect kafka success")

	err = logicReplicator.BeginReplication(ctx)
	if err != nil {
		logger.ErrorWith(ctx, err).Msg("BeginReplication error")
	}

	logger.Info(ctx).Msg("pg replication stopped")

}
