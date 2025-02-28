---
title: Deploy Databend With Tencent Cloud COS
sidebar_label: With Tencent COS
description:
  How to deploy Databend with Tencent Cloud(腾讯云) COS.
---
import GetLatest from '@site/src/components/GetLatest';

:::tip

Expected deployment time: ** 5 minutes ⏱ **

:::

This guideline will deploy Databend(standalone) with Tencent Cloud(腾讯云) COS step by step.
<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/deploy-cos-standalone.png" width="300"/>
</p>


### Before you begin

* **COS:** Tencent Cloud COS is a S3-like object storage.
  * [How to Create COS Bucket](https://cloud.tencent.com/document/product/436/13309)
  * [How to Get COS access_key_id and secret_access_key](https://cloud.tencent.com/document/product/436/68282)

## 1. Download

You can find the latest binaries on the [github release](https://github.com/datafuselabs/databend/releases) page or [build from source](../60-contributing/00-building-from-source.md).

```shell
mkdir databend && cd databend
```
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Linux">

```shell
curl -LJO https://github.com/datafuselabs/databend/releases/download/${version}/databend-${version}-x86_64-unknown-linux-musl.tar.gz
```

</TabItem>
</Tabs>

<Tabs groupId="operating-systems">
<TabItem value="linux" label="Linux">

```shell
tar xzvf databend-${version}-x86_64-unknown-linux-musl.tar.gz
```

</TabItem>
</Tabs>

## 2. Deploy databend-meta (Standalone)

databend-meta is a global service for the meta data(such as user, table schema etc.).

### 2.1 Create databend-meta.toml

```shell title="databend-meta.toml"
dir = "metadata/_logs"
admin_api_address = "127.0.0.1:8101"
grpc_api_address = "127.0.0.1:9101"

[raft_config]
id = 1
single = true
raft_dir = "metadata/datas"
```

### 2.2 Start the databend-meta 

```shell
./databend-meta -c ./databend-meta.toml > meta.log 2>&1 &
```

### 2.3 Check databend-meta 

```shell
curl -I  http://127.0.0.1:8101/v1/health
```

Check the response is `HTTP/1.1 200 OK`.


## 3. Deploy databend-query (Standalone)

### 3.1 Create databend-query.toml

```shell title="databend-query.toml"
[log]
level = "INFO"
dir = "benddata/_logs"

[query]
# For admin RESET API.
admin_api_address = "127.0.0.1:8001"

# Metrics.
metric_api_address = "127.0.0.1:7071"

# Cluster flight RPC.
flight_api_address = "127.0.0.1:9091"

# Query MySQL Handler.
mysql_handler_host = "127.0.0.1"
mysql_handler_port = 3307

# Query ClickHouse Handler.
clickhouse_handler_host = "127.0.0.1"
clickhouse_handler_port = 9001

# Query HTTP Handler.
http_handler_host = "127.0.0.1"
http_handler_port = 8081

tenant_id = "tenant1"
cluster_id = "cluster1"

[meta]
address = "127.0.0.1:9101"
username = "root"
password = "root"

[storage]
# s3
type = "s3"

[storage.s3]
# How to create a bucket:
# https://cloud.tencent.com/document/product/436/13309
// highlight-next-line
bucket = "databend-1253727613"

# You can get the URL from the bucket detail page.
// highlight-next-line
endpoint_url = "https://cos.ap-beijing.myqcloud.com"

# How to get access_key_id and secret_access_key:
# https://cloud.tencent.com/document/product/436/68282
// highlight-next-line
access_key_id = "<your-key-id>"
// highlight-next-line
secret_access_key = "<your-access-key>"
```

:::tip
In this example COS region is `ap-beijing`.
:::

### 3.2 Start databend-query

```shell
./databend-query -c ./databend-query.toml > query.log 2>&1 &
```

### 3.3 Check databend-query

```shell
curl -I  http://127.0.0.1:8001/v1/health
```

Check the response is `HTTP/1.1 200 OK`.

## 4. Play

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```sql
CREATE TABLE t1(a INT);
```

```sql
INSERT INTO t1 VALUES(1), (2);
```

```sql
SELECT * FROM T1;
```
```text
  +------+
  | a    |
  +------+
  |    1 |
  |    2 |
  +------+
```

<GetLatest/>