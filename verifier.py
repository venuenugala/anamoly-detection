# verifier.py
from merkle import verify_proof
import json

def verify_record(payload, index: int):
    leaf = bytes.fromhex(payload['leaf_hashes'][index])
    root = bytes.fromhex(payload['merkle_root'])
    proof = [(bytes.fromhex(h), r) for h, r in payload['proofs'][str(index)]]
    return verify_proof(root, leaf, index, proof)

# In Kafka Consumer
for msg in consumer:
    payload = json.loads(msg.value)
    anomalies = [i for i in range(len(payload['records'])) if not verify_record(payload, i)]
    if anomalies:
        trigger_reconciliation(payload['meta']['gcs_uri'], payload['meta']['generation'], anomalies)
