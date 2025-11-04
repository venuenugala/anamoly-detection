# record_indexer.py
from typing import List, Tuple
from google.cloud import storage

def build_byte_offsets(blob, generation: int) -> List[Tuple[int, int]]:
    """
    Returns list of (start, end) byte offsets for each record
    Assumes newline-delimited (CSV, JSONL)
    """
    offsets = []
    start = 0
    with blob.open('rb', generation=generation) as f:
        for line in f:
            end = f.tell()
            if line.strip():  # skip empty
                offsets.append((start, end))
            start = end
    return offsets
