"""
Scenario injection engine for rules R001 to R005.
"""
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from .config import HIGH_AMOUNT_THRESHOLDS, SUPPORTED_COUNTRIES

def inject_fraud_scenarios(
    customers: List[Dict[str, Any]],
    devices: List[Dict[str, Any]],
    start_time: datetime,
    seed: int,
    target_tx_count: int = 0
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Injects reproducible scenarios for R001, R002, R003, R004, R005.
    Returns (scenario_transactions, scenario_expectations, scenario_counts)
    """
    rnd = random.Random(seed + 2)
    txs = []
    exp = []
    counts = {"R001": 0, "R002": 0, "R003": 0, "R004": 0, "R005": 0}
    
    tx_counter = 1
    
    # Map customer to their registered devices
    c_devices = {}
    for d in devices:
        cid = d["customer_id"]
        c_devices.setdefault(cid, []).append(d)
        
    c_map = {c["customer_id"]: c for c in customers}
    customer_ids = [c["customer_id"] for c in customers]

    # 1. R001 Scenario (Transaction Burst): 5 approved transactions in 8 minutes for a customer
    c_r001 = rnd.choice(customer_ids)
    c_r001_info = c_map[c_r001]
    dev_r001 = c_devices.get(c_r001, devices)[0]["device_id"]
    t0_r001 = start_time + timedelta(hours=2)
    
    for k in range(5):
        tid = f"T{tx_counter:06d}"
        tx_counter += 1
        ts = t0_r001 + timedelta(minutes=k * 1.5)
        txs.append({
            "transaction_id": tid,
            "customer_id": c_r001,
            "device_id": dev_r001,
            "transaction_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "amount": "50.00",
            "currency": "EUR",
            "merchant_id": "M0001",
            "merchant_category": "RETAIL",
            "country": c_r001_info["home_country"],
            "status": "APPROVED"
        })
        is_r001_trigger = (k >= 4)  # 5th transaction activates burst count >= 5
        exp.append({
            "transaction_id": tid,
            "scenario_id": "SCENARIO_R001_BURST",
            "expected_r001": is_r001_trigger,
            "expected_r002": False,
            "expected_r003": False,
            "expected_r004": False,
            "expected_r005": False,
            "expected_risk_score": 20 if is_r001_trigger else 0,
            "expected_risk_level": "MEDIUM" if is_r001_trigger else "LOW",
            "valid_for_rule_testing": True
        })
    counts["R001"] += 1

    # 2. R002 Scenario (High Amount): Approved amount above customer risk profile threshold
    c_r002 = rnd.choice(customer_ids)
    c_r002_info = c_map[c_r002]
    prof = c_r002_info["risk_profile"]
    high_amt = HIGH_AMOUNT_THRESHOLDS[prof] + 500.00
    dev_r002 = c_devices.get(c_r002, devices)[0]["device_id"]
    t0_r002 = start_time + timedelta(hours=4)
    
    tid_r002 = f"T{tx_counter:06d}"
    tx_counter += 1
    txs.append({
        "transaction_id": tid_r002,
        "customer_id": c_r002,
        "device_id": dev_r002,
        "transaction_timestamp": t0_r002.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "amount": f"{high_amt:.2f}",
        "currency": "EUR",
        "merchant_id": "M0002",
        "merchant_category": "ONLINE",
        "country": c_r002_info["home_country"],
        "status": "APPROVED"
    })
    exp.append({
        "transaction_id": tid_r002,
        "scenario_id": "SCENARIO_R002_HIGH_AMOUNT",
        "expected_r001": False,
        "expected_r002": True,
        "expected_r003": False,
        "expected_r004": False,
        "expected_r005": False,
        "expected_risk_score": 30,
        "expected_risk_level": "MEDIUM",
        "valid_for_rule_testing": True
    })
    counts["R002"] += 1

    # 3. R003 Scenario (Rapid Country Change): Two consecutive approved transactions in different countries within 30 min
    c_r003 = rnd.choice(customer_ids)
    c_r003_info = c_map[c_r003]
    dev_r003 = c_devices.get(c_r003, devices)[0]["device_id"]
    t0_r003 = start_time + timedelta(hours=6)
    c1 = c_r003_info["home_country"]
    c2 = "FR" if c1 != "FR" else "ES"

    # Event 1
    tid_r003_1 = f"T{tx_counter:06d}"
    tx_counter += 1
    txs.append({
        "transaction_id": tid_r003_1,
        "customer_id": c_r003,
        "device_id": dev_r003,
        "transaction_timestamp": t0_r003.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "amount": "100.00",
        "currency": "EUR",
        "merchant_id": "M0003",
        "merchant_category": "RETAIL",
        "country": c1,
        "status": "APPROVED"
    })
    exp.append({
        "transaction_id": tid_r003_1,
        "scenario_id": "SCENARIO_R003_RAPID_COUNTRY_EVENT1",
        "expected_r001": False,
        "expected_r002": False,
        "expected_r003": False,
        "expected_r004": False,
        "expected_r005": False,
        "expected_risk_score": 0,
        "expected_risk_level": "LOW",
        "valid_for_rule_testing": True
    })

    # Event 2 (30 mins later in c2)
    tid_r003_2 = f"T{tx_counter:06d}"
    tx_counter += 1
    txs.append({
        "transaction_id": tid_r003_2,
        "customer_id": c_r003,
        "device_id": dev_r003,
        "transaction_timestamp": (t0_r003 + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "amount": "150.00",
        "currency": "EUR",
        "merchant_id": "M0004",
        "merchant_category": "TRAVEL",
        "country": c2,
        "status": "APPROVED"
    })
    exp.append({
        "transaction_id": tid_r003_2,
        "scenario_id": "SCENARIO_R003_RAPID_COUNTRY_EVENT2",
        "expected_r001": False,
        "expected_r002": False,
        "expected_r003": True,
        "expected_r004": False,
        "expected_r005": False,
        "expected_risk_score": 40,
        "expected_risk_level": "HIGH",
        "valid_for_rule_testing": True
    })
    counts["R003"] += 1

    # 4. R004 Scenario (Repeated Declines): 3 declined transactions in 10 minutes
    c_r004 = rnd.choice(customer_ids)
    dev_r004 = c_devices.get(c_r004, devices)[0]["device_id"]
    t0_r004 = start_time + timedelta(hours=8)

    for k in range(3):
        tid = f"T{tx_counter:06d}"
        tx_counter += 1
        ts = t0_r004 + timedelta(minutes=k * 4)
        txs.append({
            "transaction_id": tid,
            "customer_id": c_r004,
            "device_id": dev_r004,
            "transaction_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "amount": "80.00",
            "currency": "EUR",
            "merchant_id": "M0005",
            "merchant_category": "SERVICES",
            "country": "ES",
            "status": "DECLINED"
        })
        is_r004_trigger = (k >= 2)
        exp.append({
            "transaction_id": tid,
            "scenario_id": "SCENARIO_R004_REPEATED_DECLINES",
            "expected_r001": False,
            "expected_r002": False,
            "expected_r003": False,
            "expected_r004": is_r004_trigger,
            "expected_r005": False,
            "expected_risk_score": 15 if is_r004_trigger else 0,
            "expected_risk_level": "LOW",
            "valid_for_rule_testing": True
        })
    counts["R004"] += 1

    # 5. R005 Scenario (Unknown Device + R002 High Amount)
    c_r005 = rnd.choice(customer_ids)
    c_r005_info = c_map[c_r005]
    prof_r005 = c_r005_info["risk_profile"]
    amt_r005 = HIGH_AMOUNT_THRESHOLDS[prof_r005] + 300.00
    unk_device = "D999999"  # Unregistered device
    t0_r005 = start_time + timedelta(hours=10)

    tid_r005 = f"T{tx_counter:06d}"
    tx_counter += 1
    txs.append({
        "transaction_id": tid_r005,
        "customer_id": c_r005,
        "device_id": unk_device,
        "transaction_timestamp": t0_r005.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "amount": f"{amt_r005:.2f}",
        "currency": "EUR",
        "merchant_id": "M0006",
        "merchant_category": "ONLINE",
        "country": c_r005_info["home_country"],
        "status": "APPROVED"
    })
    # R002 (30) + R005 (10) = 40 (HIGH)
    exp.append({
        "transaction_id": tid_r005,
        "scenario_id": "SCENARIO_R005_UNKNOWN_DEVICE_COMBINED",
        "expected_r001": False,
        "expected_r002": True,
        "expected_r003": False,
        "expected_r004": False,
        "expected_r005": True,
        "expected_risk_score": 40,
        "expected_risk_level": "HIGH",
        "valid_for_rule_testing": True
    })
    counts["R005"] += 1

    # Generate background ordinary transactions up to target_tx_count
    remaining = target_tx_count - len(txs)
    if remaining > 0:
        merchant_cats = ["RETAIL", "ONLINE", "TRAVEL", "FOOD", "ENTERTAINMENT", "SERVICES"]
        for _ in range(remaining):
            cid = rnd.choice(customer_ids)
            c_info = c_map[cid]
            cust_devs = c_devices.get(cid, [])
            if cust_devs and rnd.random() < 0.95:
                did = rnd.choice(cust_devs)["device_id"]
            else:
                did = f"D_UNK_{rnd.randint(1000, 9999)}"

            offset_secs = rnd.randint(0, 30 * 24 * 3600)
            ts = start_time + timedelta(seconds=offset_secs)
            amt = round(rnd.uniform(5.0, 500.0), 2)
            country = c_info["home_country"] if rnd.random() < 0.90 else rnd.choice(SUPPORTED_COUNTRIES)
            status = "APPROVED" if rnd.random() < 0.95 else "DECLINED"

            tid = f"T{tx_counter:06d}"
            tx_counter += 1

            txs.append({
                "transaction_id": tid,
                "customer_id": cid,
                "device_id": did,
                "transaction_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "amount": f"{amt:.2f}",
                "currency": "EUR",
                "merchant_id": f"M{rnd.randint(1, 100):04d}",
                "merchant_category": rnd.choice(merchant_cats),
                "country": country,
                "status": status
            })

    return txs, exp, counts
