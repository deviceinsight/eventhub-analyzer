import os
import datetime

import jsonpickle
import more_itertools
from azure.storage.blob import BlobServiceClient
import itertools
from dotenv import load_dotenv

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


def run_checkpoint_anaylysis(current_timestamp, current_event_hubs, previous_timestamp, previous_event_hubs):
    difference_in_seconds = (current_timestamp - previous_timestamp).total_seconds()
    for event_hub_name in current_event_hubs:
        for consumer_group_name in current_event_hubs[event_hub_name]:
            for partition_id in current_event_hubs[event_hub_name][consumer_group_name]:
                current_checkpoint = current_event_hubs[event_hub_name][consumer_group_name][partition_id]
                try:
                    previous_checkpoint = previous_event_hubs[event_hub_name][consumer_group_name][partition_id]
                except KeyError:
                    previous_checkpoint = None

                if previous_checkpoint is None:
                    print(f'{event_hub_name}/{consumer_group_name}/{partition_id}: No previous data')
                    continue

                sequence_delta = current_checkpoint.sequence_number - previous_checkpoint.sequence_number
                offset_delta = current_checkpoint.offset - previous_checkpoint.offset

                events_per_second = sequence_delta / difference_in_seconds
                bytes_per_second = offset_delta / difference_in_seconds
                print(f'{event_hub_name}/{consumer_group_name}/{partition_id}: {events_per_second:.2f} events per second, {bytes_per_second:.0f} bytes per second')


def main():
    previous_data = load_persisted_data()

    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    offset_container_name = os.getenv('CONTAINER_NAME')

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container=offset_container_name)
    blob_list = container_client.list_blobs(include='metadata')
    raw_checkpoints = []
    ownerships = []

    for blob in blob_list:
        name = blob.name
        _, event_hub_name, consumer_group_name, entity, partition_id = name.split('/')

        if entity == 'checkpoint':
            raw_checkpoints.append(
                RawCheckpoint(event_hub_name, consumer_group_name, partition_id, int(blob.metadata['sequencenumber']),
                              int(blob.metadata['offset'])))

        if entity == 'ownership':
            ownerships.append(Ownership(event_hub_name, consumer_group_name, partition_id, blob.metadata['ownerid']))

    event_hubs = {}
    raw_checkpoints_by_event_hub = itertools.groupby(raw_checkpoints, lambda c: c.event_hub)
    for event_hub_name, raw_checkpoints_of_event_hub in raw_checkpoints_by_event_hub:
        if event_hub_name not in event_hubs:
            event_hubs[event_hub_name] = {}

        raw_checkpoints_by_consumer_group = itertools.groupby(raw_checkpoints_of_event_hub, lambda c: c.consumer_group)
        for consumer_group_name, raw_checkpoints_of_consumer_group in raw_checkpoints_by_consumer_group:

            checkpoints_by_partition_id = {}
            for raw_checkpoint in raw_checkpoints_of_consumer_group:
                checkpoints_by_partition_id[raw_checkpoint.partition_id] = Checkpoint(offset=raw_checkpoint.offset,
                                                                                      sequence_number=raw_checkpoint.sequence_number)

            event_hubs[event_hub_name][consumer_group_name] = checkpoints_by_partition_id

    print(jsonpickle.encode(event_hubs, indent=2))

    persist_data(event_hubs)
    if previous_data is None:
        print("No previous run found, cannot perform analysis. Wait a minute and run this command again.")
    else:
        previous_timestamp = datetime.datetime.fromisoformat(previous_data.timestamp)
        run_checkpoint_anaylysis(now(), event_hubs, previous_timestamp, previous_data.event_hubs)
    """
    # TODO: Group by event_hub, consumer group
    ownership_by_owner_id = itertools.groupby(ownerships, lambda o: o.owner_id)
    for owner_id, ownership in ownership_by_owner_id:
        print(owner_id, len(list(ownership)))

    total_sequence_number = sum([checkpoint.sequence_number for checkpoint in checkpoints])
    min_sequence_number = min([checkpoint.sequence_number for checkpoint in checkpoints])
    max_sequence_number = max([checkpoint.sequence_number for checkpoint in checkpoints])
    avg_sequence_number = total_sequence_number / len(checkpoints)

    for checkpoint in checkpoints:
        percentage = round((checkpoint.sequence_number / avg_sequence_number) * 100)
        print(f"Partition {checkpoint.partition_id} has {percentage}% of average")

    persist_checkpoints(checkpoints)
    """


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


if __name__ == '__main__':
    main()
    # persist_checkpoints(
    #     [Checkpoint(event_hub='123', partition_id=0, consumer_group='456', offset=1, sequence_number=2)])
    # load_persisted_checkpoints()
