#!/bin/bash

docker run --rm -e CLICKHOUSE_DB=default -e CLICKHOUSE_USER=pbstck -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 -e CLICKHOUSE_PASSWORD=pbstck -p 8123:8123 -p 9000:9000/tcp clickhouse