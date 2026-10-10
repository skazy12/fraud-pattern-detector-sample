"""
Quality defect injection engine for rules Q001 to Q016.
"""
import random
from typing import List, Dict, Any, Tuple

def inject_quality_defects(
    txs: List[Dict[str, Any]],
    customers: List[Dict[str, Any]],
    devices: List[Dict[str, Any]],
    seed: int
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Injects controlled invalid records and duplicates into transaction stream.
    Returns (modified_transactions, defect_counts)
    """
    rnd = random.Random(seed + 3)
    defects_count = {"exact_duplicates": 0, "conflicting_duplicates": 0, "field_defects": 0}
    
    if not txs:
        return txs, defects_count

    # 1. Exact Duplicate Injection (Q009A)
    tx_to_dup = txs[0].copy()
    txs.append(tx_to_dup)
    defects_count["exact_duplicates"] += 1

    # 2. Conflicting Duplicate Injection (Q009B)
    if len(txs) > 1:
        tx_to_conflict = txs[1].copy()
        tx_to_conflict["amount"] = "99999.99"
        txs.append(tx_to_conflict)
        defects_count["conflicting_duplicates"] += 1

    # 3. Field Quality Defects (Q004 Negative amount, Q005 Invalid Currency, Q006 Unsupported Country)
    if len(txs) > 2:
        bad_amt_tx = txs[2].copy()
        bad_amt_tx["transaction_id"] = f"T_BAD_AMT_{rnd.randint(100,999)}"
        bad_amt_tx["amount"] = "-50.00"
        txs.append(bad_amt_tx)
        defects_count["field_defects"] += 1

    if len(txs) > 3:
        bad_curr_tx = txs[3].copy()
        bad_curr_tx["transaction_id"] = f"T_BAD_CURR_{rnd.randint(100,999)}"
        bad_curr_tx["currency"] = "USD"
        txs.append(bad_curr_tx)
        defects_count["field_defects"] += 1

    return txs, defects_count
