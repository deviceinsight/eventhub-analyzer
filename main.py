import os
import datetime

import jsonpickle
from azure.storage.blob import BlobServiceClient
import itertools
from dotenv import load_dotenv

load_dotenv()


class EventHub:
    def __init__(self, name, consumer_groups):
        self.name = name
        self.consumer_groups = consumer_groups


class ConsumerGroup:
    def __init__(self, name, checkpoints):
        self.name = name
        self.checkpoints = checkpoints


class PersistedCheckpoints:
    def __init__(self, timestamp, checkpoints):
        self.checkpoints = checkpoints
        self.timestamp = timestamp


class Checkpoint:
    def __init__(self, event_hub, consumer_group, partition_id, sequence_number, offset):
        self.offset = offset
        self.sequence_number = sequence_number
        self.partition_id = partition_id
        self.consumer_group = consumer_group
        self.event_hub = event_hub


class Ownership:
    def __init__(self, event_hub, consumer_group, partition_id, owner_id):
        self.owner_id = owner_id
        self.partition_id = partition_id
        self.consumer_group = consumer_group
        self.event_hub = event_hub


def main():
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    offset_container_name = os.getenv('CONTAINER_NAME')

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container=offset_container_name)
    blob_list = container_client.list_blobs(include='metadata')
    checkpoints = []
    ownerships = []
    for blob in blob_list:
        name = blob.name
        _, event_hub, consumer_group, entity, partition_id = name.split('/')
        if entity == 'checkpoint':
            checkpoints.append(Checkpoint(event_hub, consumer_group, partition_id, int(blob.metadata['sequencenumber']),
                                          int(blob.metadata['offset'])))

        if entity == 'ownership':
            ownerships.append(Ownership(event_hub, consumer_group, partition_id, blob.metadata['ownerid']))

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


def persist_checkpoints(checkpoints):
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    persisted_checkpoints = PersistedCheckpoints(timestamp=timestamp, checkpoints=checkpoints)
    with open('checkpoints.json', 'w') as f:
        f.write(jsonpickle.encode(persisted_checkpoints, indent=2))


def load_persisted_checkpoints():
    with open('checkpoints.json', 'r') as f:
        return jsonpickle.decode(f.read())


if __name__ == '__main__':
    # main()
    persist_checkpoints([Checkpoint(event_hub='123', partition_id=0, consumer_group='456', offset=1, sequence_number=2)])
    load_persisted_checkpoints()
