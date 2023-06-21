# Event Hub Analyzer

Event Hub Analyzer is a small command line tool that can be used
to analyze certain aspects of Event Hubs.

## Installation

```
pip install eventhub-analyzer
```

## Checkpoints per partition

For every event hub, consumer group, the number of 
events/sequence numbers between two invocations is retrieved and
the throughput per partition is calculated. This can be used to 
determine if the load is correctly distributed among partitions.

When there are some partition that get little to no throughput while
others have large throughput, it is a sign that the partition key
is not chosen optimally and that you should try to choose a property
with a higher cardinality as the partition key.

### Usage

```bash
eventhub-analyzer checkpoints -n CONTAINER_NAME -c CONNECTION_STRING
```

You can also specify the settings via environment variables:

```bash
export STORAGE_ACCOUNT_CONNECTION_STRING='DefaultEndpointsProtocol=https;AccountName=x;AccountKey=y;EndpointSuffix=core.windows.net'
export CONTAINER_NAME='event-hub-offsets'
eventhub-analyzer checkpoints
```

### Example output

```
Event Hub: telemetry, Consumer Group: my-consumer
Event Hub   Consumer Group   Partition   Events per second
telemetry   my-consumer              0             158.034
telemetry   my-consumer              1             203.257
telemetry   my-consumer              2             148.103
telemetry   my-consumer              3               0.000
telemetry   my-consumer              4             201.780
telemetry   my-consumer              5             106.081
telemetry   my-consumer              6              72.307
telemetry   my-consumer              7             160.783
telemetry   my-consumer              8             118.351
```

As you can see, partition 3 is not getting any events and
the number of events is not well distributed overall. There
might be some gains possible by choosing a different partition key
(or by partitioning manually on the client).

### Clearing checkpoints

Example:

```bash
eventhub-analyzer clear-checkpoints --consumer-group redis-timeseries
```

## Publishing

```
poetry build
poetry publish
```
