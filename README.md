# pg-replication-kafka
PostgreSQL logical replication to Kafka

[go-binlog-kafka](https://github.com/feiin/go-binlog-kafka)PostgreSQL版本,将pg数据库的WAL日志解析成JSON推送至Kafka消息队列

## Getting Started

最简单方式启动查看

```
./pg-replication-kafka -host=10.0.0.1 -port=5432 -user=postgres -password=postgrespass -db=test -pubname=test -kafka_addr=10.0.0.1:9092 -kafka_topic_name=postgres

``` 

## 推送的JSON格式数据

每条数据变更(insert/update/delete)都会解析成以下格式JSON推送至配置的kafka的topic
```
{
    "log_pos": 33827184, //  uint64
    "action": "insert", // insert/update/delete/DDL action
    "schema": "tests", // 库名称
    "namespace":"public"  // namespace
    "table": "tests",  // 表名称
    "gtid": "884",// xid  transaction id
    "timestamp": 1713158815, // event unix时间戳
    "values": null, // insert/delete 时是对应行数据
    "before_values":{...} // update 变更前行数据
    "after_values":{...} // update 变更后行数据
}
```

```
# insert时，推送格式数据如下

{"action":"insert","gtid":"885","log_pos":33828584,"namespace":"public","schema":"test","table":"article","timestamp":1735354300,"values":{"content":"this is content","id":3,"title":"title test"}}

# update时，推送格式如下

{"action":"update","after_values":{"content":"this is content","id":3,"title":"title test"},"before_values":{"content":"this is content 2024","id":3,"title":"title test"},"gtid":"888","log_pos":33832840,"namespace":"public","schema":"test","table":"article","timestamp":1735354352}

# delete时,推送数据如下

{"action":"delete","gtid":"889","log_pos":33833064,"namespace":"public","schema":"test","table":"article","timestamp":1735354378,"values":{"content":"this is content","id":3,"title":"title test"}}

```

## Options

确保数据库wal_level为逻辑复制logical

```postgres
show wal_level;  检查是否为logical
```

自建可以修改conf文件:
```
# change requires restart
wal_level = logical
max_wal_senders = 10		# max number of walsender processes
max_replication_slots = 10	# max number of replication slots
```
RDS云服务如需配置见云产商说明，如阿里云 https://help.aliyun.com/zh/rds/apsaradb-rds-for-postgresql/logical-subscription


- kafka_addr 推送目标kafka的地址
- kafka_topic_name 推送目标的kafka topic名称
- host postgres源库地址
- port postgres源库端口
- postgres postgres源库用户名
- password postgres源库密码
- db postgres源库database实例名称
- pubname publication name，通过下面语句创建发布名称,源库里执行:
    ```
    # CREATE PUBLICATION <发布名称> FOR TABLE <表名>;

    CREATE PUBLICATION test FOR ALL TABLES

    # view 
    SELECT * FROM pg_publication;
    ```

    如果update的action需要before_values需要修改表的REPLICA IDENTITY(复制标识)配置模式为FULL，否则before_values为空

    ```
    ALTER TABLE test_tablename REPLICA IDENTITY FULL;

    ```

- slot_name 逻辑复制槽,程序默认为`pg_replicate_kafka`
- debug 开启调试模式时，会输出推送kafka消息详情



 