import os
import datetime
import statistics

import click
import jsonpickle
from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubConsumerClient
import itertools
from dotenv import load_dotenv
from texttable import Texttable

load_dotenv()


class CheckpointData:
    def __init__(self, timestamp, event_hubs):
        self.timestamp = timestamp
        self.event_hubs = event_hubs


class Checkpoint:
    def __init__(self, sequence_number, offset):
        self.offset = offset
        self.sequence_number = sequence_number


class RawCheckpoint:
    def __init__(self, event_hub, consumer_group, partition_id, sequence_number, offset):
        self.event_hub = event_hub
        self.consumer_group = consumer_group
        self.offset = offset
        self.sequence_number = sequence_number
        self.partition_id = partition_id


class Ownership:
    def __init__(self, event_hub, consumer_group, partition_id, owner_id):
        self.owner_id = owner_id
        self.partition_id = partition_id
        self.consumer_group = consumer_group
        self.event_hub = event_hub


def run_checkpoint_analysis(current_timestamp, current_event_hubs, previous_timestamp, previous_event_hubs,
                            event_hub_filter, consumer_group_filter):
    difference_in_seconds = (current_timestamp - previous_timestamp).total_seconds()
    for event_hub_name in current_event_hubs:

        if event_hub_filter is not None and event_hub_filter != event_hub_name:
            continue

        for consumer_group_name in current_event_hubs[event_hub_name]:

            if consumer_group_filter is not None and consumer_group_filter != consumer_group_name:
                continue

            table = Texttable()
            table.set_deco(Texttable.HEADER)
            table.set_cols_dtype(['t',
                                  't',
                                  'i',
                                  'i',
                                  'f'])
            table.set_cols_align(["l", "l", "r", "r", "r"])
            table.add_row(["Event Hub", "Consumer Group", "Partition", "Sequence number", "Events per second"])
            partition_ids = sorted(current_event_hubs[event_hub_name][consumer_group_name], key=lambda p: int(p))
            events_per_seconds_stats = []
            for partition_id in partition_ids:
                current_checkpoint = current_event_hubs[event_hub_name][consumer_group_name][partition_id]
                try:
                    previous_checkpoint = previous_event_hubs[event_hub_name][consumer_group_name][partition_id]
                    sequence_delta = current_checkpoint.sequence_number - previous_checkpoint.sequence_number
                    events_per_second = sequence_delta / difference_in_seconds
                    events_per_seconds_stats.append(events_per_second)
                except KeyError:
                    events_per_second = -1

                table.add_row([event_hub_name, consumer_group_name, partition_id, current_checkpoint.sequence_number,
                               events_per_second])

            click.echo(table.draw())
            click.echo()
            click.echo("Events per second stats:")
            click.echo(f"Min: {min(events_per_seconds_stats):.3f}")
            click.echo(f"Max: {max(events_per_seconds_stats):.3f}")
            click.echo(f"Avg: {sum(events_per_seconds_stats) / len(events_per_seconds_stats):.3f}")
            click.echo(f"StdDev: {statistics.stdev(events_per_seconds_stats):.3f}")
            click.echo()


def checkpoint_analysis(connection_string, container_name, event_hub_filter, consumer_group_filter):
    previous_data = load_persisted_data()

    raw_checkpoints = get_data_from_container('checkpoint', connection_string, container_name)

    event_hubs = group_raw_checkpoints(raw_checkpoints)

    persist_data(event_hubs)
    if previous_data is None:
        click.echo("No previous run found, cannot perform analysis. Wait a minute and run this command again.")
    else:
        previous_timestamp = datetime.datetime.fromisoformat(previous_data.timestamp)
        run_checkpoint_analysis(now(), event_hubs, previous_timestamp, previous_data.event_hubs, event_hub_filter,
                                consumer_group_filter)


def clear_checkpoint_operation(connection_string, container_name, event_hub_filter, consumer_group_filter):
    if consumer_group_filter is None:
        print("You must specify a consumer group")
        return
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container=container_name)
    blob_list = container_client.list_blobs(include='metadata')
    for blob in blob_list:
        name = blob.name
        _, event_hub_name, consumer_group_name, entity, partition_id = name.split('/')
        if consumer_group_name == consumer_group_filter and entity == 'checkpoint':
            print(f"Deleting blob: {name}")
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=name)
            blob_client.delete_blob()


def group_raw_checkpoints(raw_checkpoints):
    """
    Groups the checkpoints in nested dicts: event_hub -> consumer_group -> partition_id
    :param raw_checkpoints:
    :return:
    """
    event_hubs = {}
    raw_checkpoints_by_event_hub = itertools.groupby(raw_checkpoints, lambda c: c.event_hub)
    for event_hub_name, raw_checkpoints_of_event_hub in raw_checkpoints_by_event_hub:

        if event_hub_name not in event_hubs:
            event_hubs[event_hub_name] = {}

        raw_checkpoints_by_consumer_group = itertools.groupby(raw_checkpoints_of_event_hub, lambda c: c.consumer_group)
        for consumer_group_name, raw_checkpoints_of_consumer_group in raw_checkpoints_by_consumer_group:

            checkpoints_by_partition_id = {}
            for raw_checkpoint in raw_checkpoints_of_consumer_group:
                checkpoint = Checkpoint(offset=raw_checkpoint.offset, sequence_number=raw_checkpoint.sequence_number)
                checkpoints_by_partition_id[raw_checkpoint.partition_id] = checkpoint

            event_hubs[event_hub_name][consumer_group_name] = checkpoints_by_partition_id
    return event_hubs


def lag_analysis(storage_connection_string, container_name, event_hub, consumer_group, event_hub_connection_string):
    raw_checkpoints = get_data_from_container('checkpoint', storage_connection_string, container_name)
    grouped_checkpoints = group_raw_checkpoints(raw_checkpoints)
    print(grouped_checkpoints)
    relevant_checkpoints = grouped_checkpoints[event_hub][consumer_group]

    event_hub_client = EventHubConsumerClient.from_connection_string(event_hub_connection_string,
                                                                     eventhub_name=event_hub,
                                                                     consumer_group=consumer_group)

    table = Texttable()
    table.set_deco(Texttable.HEADER)
    table.set_cols_dtype(['t',
                          't',
                          'i',
                          'i',
                          'i',
                          'i'])
    table.set_cols_align(["l", "l", "r", "r", "r", "r"])
    table.add_row(["Event Hub",
                   "Consumer Group",
                   "Partition",
                   "Read/Committed Sequence number",
                   "Written/Enqueued Sequence Number",
                   "Lag",
                   ])
    for partition_id in event_hub_client.get_partition_ids():
        print(f"Getting data for partition {partition_id}")
        partition_properties = event_hub_client.get_partition_properties(partition_id)
        read_sequence_number = relevant_checkpoints[partition_id].sequence_number
        written_sequence_number = partition_properties['last_enqueued_sequence_number']
        lag = written_sequence_number - read_sequence_number

        table.add_row([event_hub,
                       consumer_group,
                       partition_id,
                       read_sequence_number,
                       written_sequence_number,
                       lag])

    click.echo(table.draw())
    click.echo()


def get_data_from_container(entity_to_get, connection_string, container_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container=container_name)
    blob_list = container_client.list_blobs(include='metadata')
    result = []
    for blob in blob_list:
        name = blob.name
        _, event_hub_name, consumer_group_name, entity, partition_id = name.split('/')

        if entity_to_get == entity == 'checkpoint':
            sequence_number = int(blob.metadata['sequencenumber']) if blob.metadata is not None else 0
            offset = int(blob.metadata['offset']) if blob.metadata is not None else 0
            checkpoint = RawCheckpoint(event_hub_name, consumer_group_name, partition_id, sequence_number, offset)
            result.append(checkpoint)

        if entity_to_get == entity == 'ownership':
            ownership = Ownership(event_hub_name, consumer_group_name, partition_id, blob.metadata['ownerid'])
            result.append(ownership)
    return result


def persist_data(event_hubs):
    timestamp = now().isoformat()
    persisted_data = CheckpointData(timestamp=timestamp, event_hubs=event_hubs)
    with open('data.json', 'w') as f:
        f.write(jsonpickle.encode(persisted_data, indent=2))


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def load_persisted_data():
    if not os.path.isfile('data.json'):
        return None
    with open('data.json', 'r') as f:
        return jsonpickle.decode(f.read())


def owner_analysis(connection_string, container_name):
    ownerships = get_data_from_container('ownership', connection_string, container_name)

    event_hubs = {}
    ownerships_by_event_hub = itertools.groupby(ownerships, lambda c: c.event_hub)
    for event_hub_name, ownerships_of_event_hub in ownerships_by_event_hub:
        if event_hub_name not in event_hubs:
            event_hubs[event_hub_name] = {}

        ownerships_by_consumer_group = itertools.groupby(ownerships_of_event_hub, lambda c: c.consumer_group)
        for consumer_group_name, ownerships_of_consumer_group in ownerships_by_consumer_group:

            click.echo(f"Event Hub: {event_hub_name}, Consumer Group: {consumer_group_name}")

            ownerships_by_owner_id = itertools.groupby(ownerships_of_consumer_group, lambda o: o.owner_id)
            owner_count = 0
            for owner_id, ownerships_of_owner in ownerships_by_owner_id:
                click.echo(f"{owner_id} owns {len(list(ownerships_of_owner))} partitions")
                owner_count += 1
            click.echo(f"{owner_count} owners in total")
            click.echo()


def event_hub_option(function, *args, **kwargs):
    function = click.option('-e', '--event-hub', *args, required=False,
                            envvar='EVENT_HUB',
                            help='The name of the event hub to analyze. If not specified, show all '
                                 'event hubs.', **kwargs)(function)

    return function


def connection_string_option(function):
    function = click.option('-c', '--connection-string', required=True,
                            envvar='STORAGE_ACCOUNT_CONNECTION_STRING',
                            help='The connection string of the storage account. Can instead be given '
                                 'using the STORAGE_ACCOUNT_CONNECTION_STRING environment variable.')(function)

    return function


def container_name_option(function):
    function = click.option('-n', '--container-name',
                            required=True,
                            envvar='CONTAINER_NAME',
                            help='The name of the container in which the event hub offsets are '
                                 'stored. Can instead be given using the CONTAINER_NAME '
                                 'environment variable.')(function)

    return function


def consumer_group_option(function):
    function = click.option('-g', '--consumer-group',
                            required=False,
                            envvar='CONSUMER_GROUP',
                            help='The name of the consumer group to analyze. If not specified, '
                                 'show all consumer groups.')(function)

    return function


@click.group()
def cli():
    pass


@connection_string_option
@consumer_group_option
@event_hub_option
@container_name_option
@cli.command(help="Analyze checkpoints per partition")
def checkpoints(connection_string, container_name, event_hub, consumer_group):
    checkpoint_analysis(connection_string, container_name, event_hub, consumer_group)


@connection_string_option
@consumer_group_option
@event_hub_option
@container_name_option
@cli.command(help="Clear checkpoints")
def clear_checkpoints(connection_string, container_name, event_hub, consumer_group):
    clear_checkpoint_operation(connection_string, container_name, event_hub, consumer_group)


@connection_string_option
@event_hub_option
@container_name_option
@cli.command(help="Analyze owners of partitions")
def owners(connection_string, container_name, event_hub):
    owner_analysis(connection_string, container_name)


@connection_string_option
@container_name_option
@click.option('-e', '--event-hub', required=True, envvar='EVENT_HUB', help='The name of the event hub to analyze.')
@click.option('-g', '--consumer-group', required=True, envvar='CONSUMER_GROUP',
              help='The name of the consumer group to analyze.')
@click.option('-E', '--event-hub-connection-string', envvar='EVENT_HUB_CONNECTION_STRING', )
@cli.command(help="Analyze lag")
def lags(connection_string, container_name, event_hub, consumer_group, event_hub_connection_string):
    lag_analysis(connection_string, container_name, event_hub, consumer_group, event_hub_connection_string)


cli()
