"""
Customer dataset generator.
"""
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
from .config import SUPPORTED_COUNTRIES, RISK_PROFILES

def generate_customers(count: int, seed: int, start_time: datetime) -> List[Dict[str, Any]]:
    rnd = random.Random(seed)
    customers = []
    
    for i in range(1, count + 1):
        cid = f"C{i:06d}"
        home_country = rnd.choice(SUPPORTED_COUNTRIES)
        risk_profile = rnd.choice(RISK_PROFILES)
        # Created in the 30 days prior to observation start
        created_delta_sec = rnd.randint(0, 30 * 24 * 3600)
        created_at = start_time - timedelta(seconds=created_delta_sec)
        
        customers.append({
            "customer_id": cid,
            "home_country": home_country,
            "risk_profile": risk_profile,
            "created_at": created_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        })
    return customers
