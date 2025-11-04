# merkle.py
import hashlib
from typing import List, Tuple, Dict, Optional

def _hash_pair(left: bytes, right: bytes) -> bytes:
    return hashlib.sha256(left + right).digest()

def build_merkle_tree(records: List[bytes]) -> Tuple[bytes, Dict[int, List[Tuple[bytes, bool]]]]:
    """
    Returns: (root_hash, proofs)
    proofs[i] = list of (sibling_hash, is_right_sibling)
    """
    if not records:
        return hashlib.sha256(b"empty").digest(), {}

    leaves = [hashlib.sha256(r).digest() for r in records]
    n = len(leaves)
    tree = leaves[:]
    proofs: Dict[int, List[Tuple[bytes, bool]]] = {}

    level = 0
    offset = 0
    while n > 1:
        new_n = (n + 1) // 2
        new_tree = [None] * new_n
        for i in range(0, n, 2):
            left = tree[i]
            right = tree[i + 1] if i + 1 < n else left  # duplicate for odd
            parent = _hash_pair(left, right)
            new_tree[i // 2] = parent

            # Record proof for children
            if level == 0:  # leaf level
                proofs[offset + i] = [(right, True)]
                if i + 1 < n:
                    proofs[offset + i + 1] = [(left, False)]

        tree = new_tree
        n = new_n
        offset *= 2
        level += 1

    return tree[0], proofs

def verify_proof(root: bytes, leaf: bytes, index: int, proof: List[Tuple[bytes, bool]]) -> bool:
    current = leaf
    for sibling, is_right in proof:
        if is_right:
            current = _hash_pair(current, sibling)
        else:
            current = _hash_pair(sibling, current)
    return current == root
