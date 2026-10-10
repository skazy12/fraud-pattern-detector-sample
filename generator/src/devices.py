"""
Registered devices dataset generator.
"""
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
from .config import DEVICE_TYPES

def generate_devices(customers: List[Dict[str, Any]], count: int, seed: int) -> List[Dict[str, Any]]:
    rnd = random.Random(seed + 1)
    devices = []
    customer_ids = [c["customer_id"] for c in customers]
    customer_created_map = {c["customer_id"]: datetime.strptime(c["created_at"], "%Y-%m-%dT%H:%M:%SZ") for c in customers}

    for i in range(1, count + 1):
        did = f"D{i:06d}"
        cid = rnd.choice(customer_ids)
        c_created = customer_created_map[cid]
        device_type = rnd.choice(DEVICE_TYPES)
        
        # Registration date after customer creation date
        reg_delta_sec = rnd.randint(0, 15 * 24 * 3600)
        registered_at = c_created + timedelta(seconds=reg_delta_sec)

        devices.append({
            "device_id": did,
            "customer_id": cid,
            "device_type": device_type,
            "registered_at": registered_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        })
    return devices
