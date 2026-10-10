"""
Manifest and expected scenario outputs exporter.
"""
import json
import csv
from datetime import datetime
from typing import List, Dict, Any

def export_manifest(
    output_path: str,
    generator_version: str,
    seed: int,
    generation_mode: str,
    start_time: datetime,
    end_time: datetime,
    customer_count: int,
    device_count: int,
    transaction_count: int,
    scenario_counts: Dict[str, int],
    defect_counts: Dict[str, int]
):
    manifest_data = {
        "generator_version": generator_version,
        "seed": seed,
        "generation_mode": generation_mode,
        "generation_timestamp_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "observation_start_utc": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "observation_end_utc": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "customer_count": customer_count,
        "device_count": device_count,
        "transaction_count": transaction_count,
        "injected_scenario_counts": scenario_counts,
        "injected_quality_defect_counts": defect_counts
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)

def export_scenario_expected(output_path: str, expectations: List[Dict[str, Any]]):
    fieldnames = [
        "transaction_id",
        "scenario_id",
        "expected_r001",
        "expected_r002",
        "expected_r003",
        "expected_r004",
        "expected_r005",
        "expected_risk_score",
        "expected_risk_level",
        "valid_for_rule_testing"
    ]
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in expectations:
            writer.writerow(row)
