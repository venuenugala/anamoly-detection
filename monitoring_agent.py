# monitoring_agent.py
import functions_framework
from google.cloud import storage, bigquery, pubsub_v1
from kafka import KafkaProducer
import json
import os
from merkle import build_merkle_tree
from record_indexer import build_byte_offsets

storage_client = storage.Client()
bq_client = bigquery.Client()
producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BROKERS', 'localhost:9092'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@functions_framework.cloud_event
def handle_gcs_event(cloud_event):
    data = cloud_event.data
    bucket_name = data['bucket']
    object_name = data['name']
    generation = int(data['generation'])

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    # 1. Download + Parse
    content = blob.download_as_bytes(generation=generation)
    records = [line for line in content.split(b'\n') if line.strip()]

    # 2. Build Merkle Tree
    root, proofs = build_merkle_tree(records)

    # 3. GCS Metadata
    meta = {
        'gcs_uri': f"gs://{bucket_name}/{object_name}",
        'generation': generation,
        'upload_time': data['timeCreated'],
        'record_count': len(records)
    }
    final_root = hashlib.sha256(root + json.dumps(meta, sort_keys=True).encode()).digest()

    # 4. Byte Offsets
    offsets = build_byte_offsets(blob, generation)

    # 5. Prepare Payload
    payload = {
        'records': [r.decode(errors='ignore') for r in records],
        'merkle_root': final_root.hex(),
        'leaf_hashes': [hashlib.sha256(r).hexdigest() for r in records],
        'proofs': {i: [(h.hex(), r) for h, r in p] for i, p in proofs.items()},
        'offsets': offsets,
        'meta': meta
    }

    # 6. Route
    send_to_kafka(payload, object_name)
    load_to_bigquery(payload)
    write_sidecar(payload, bucket_name, object_name)

    # 7. Audit
    log_audit(meta, final_root.hex(), len(records))

    return f"Processed {object_name}#{generation}"

def send_to_kafka(payload, object_name):
    producer.send(
        'raw-records',
        value=payload,
        headers=[
            ('merkle_root', payload['merkle_root'].encode()),
            ('generation', str(payload['meta']['generation']).encode()),
            ('record_count', str(payload['meta']['record_count']).encode())
        ]
    )

def load_to_bigquery(payload):
    rows = []
    for i, rec in enumerate(payload['records']):
        rows.append({
            'data': rec,
            'record_index': i,
            'leaf_hash': payload['leaf_hashes'][i],
            'offset_start': payload['offsets'][i][0],
            'offset_end': payload['offsets'][i][1],
            'gcs_uri': payload['meta']['gcs_uri'],
            'generation': payload['meta']['generation']
        })
    bq_client.insert_rows_json('project.dataset.records_table', rows)

def write_sidecar(payload, bucket_name, object_name):
    processed_bucket = storage_client.bucket('data-lake-processed')
    sidecar = processed_bucket.blob(f"processed/{object_name}.index.json")
    sidecar.upload_from_string(json.dumps({
        'merkle_root': payload['merkle_root'],
        'leaf_hashes': payload['leaf_hashes'],
        'proofs': payload['proofs'],
        'offsets': payload['offsets'],
        'meta': payload['meta']
    }))

def log_audit(meta, root, count):
    bq_client.insert_rows_json('project.dataset.audit_log', [{
        'gcs_uri': meta['gcs_uri'],
        'generation': meta['generation'],
        'merkle_root': root,
        'record_count': count,
        'processed_at': None
    }])
