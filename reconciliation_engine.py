# reconciliation_engine.py
import networkx as nx
from google.cloud import storage
from flask import Flask, request
import json

app = Flask(__name__)
storage_client = storage.Client()
graph = nx.DiGraph([('gcs', 'kafka'), ('gcs', 'bigquery'), ('gcs', 'file')])

@app.route('/reconcile', methods=['POST'])
def reconcile():
    data = request.json
    uri = data['gcs_uri']
    gen = data['generation']
    bad_indices = data['anomalies']

    bucket_name = uri.split('/')[2]
    object_name = '/'.join(uri.split('/')[3:])
    blob = storage_client.bucket(bucket_name).blob(object_name)

    for idx in bad_indices:
        start, end = data['offsets'][idx]
        bad_record = blob.download_as_bytes(start=start, end=end, generation=gen)
        # Replay to all dependents
        for target in nx.descendants(graph, 'gcs'):
            replay_to(target, bad_record, idx, data['meta'])

    return {"status": "reconciled", "records_fixed": len(bad_indices)}

def replay_to(target, record, index, meta):
    if target == 'kafka':
        producer.send('replay-records', value={'record': record.decode(), 'index': index, 'meta': meta})
    elif target == 'bigquery':
        bq_client.insert_rows_json('project.dataset.records_table', [{
            'data': record.decode(),
            'record_index': index,
            'gcs_uri': meta['gcs_uri'],
            'generation': meta['generation']
        }])
