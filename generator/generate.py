"""
Synthetic Transaction Data Generator CLI.
"""
import os
import csv
import argparse
from datetime import datetime, timedelta
from src.config import MODE_CONFIGS, SUPPORTED_COUNTRIES, MERCHANT_CATEGORIES, TRANSACTION_STATUSES
from src.customers import generate_customers
from src.devices import generate_devices
from src.scenarios import inject_fraud_scenarios
from src.defects import inject_quality_defects
from src.manifest import export_manifest, export_scenario_expected

def write_csv(filepath: str, fieldnames: list, data: list):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic financial transaction dataset.")
    parser.add_argument("--mode", choices=["controlled", "dev", "full"], default="dev", help="Generation mode")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--output-dir", type=str, default="../data/raw", help="Output directory for CSV datasets")
    
    args = parser.parse_args()
    
    cfg = MODE_CONFIGS[args.mode]
    seed = args.seed
    out_dir = os.path.abspath(args.output_dir)
    os.makedirs(out_dir, exist_ok=True)
    
    observation_start = datetime(2026, 1, 1, 0, 0, 0)
    observation_end = observation_start + timedelta(days=30)
    
    print(f"=== Synthetic Data Generator (Mode: {args.mode}, Seed: {seed}) ===")
    print(f"Output Directory: {out_dir}")
    
    # 1. Customers
    customers = generate_customers(cfg["customer_count"], seed, observation_start)
    write_csv(
        os.path.join(out_dir, "customers.csv"),
        ["customer_id", "home_country", "risk_profile", "created_at"],
        customers
    )
    print(f"Generated {len(customers)} customers -> customers.csv")
    
    # 2. Devices
    devices = generate_devices(customers, cfg["device_count"], seed)
    write_csv(
        os.path.join(out_dir, "devices.csv"),
        ["device_id", "customer_id", "device_type", "registered_at"],
        devices
    )
    print(f"Generated {len(devices)} devices -> devices.csv")
    
    # 3. Transactions & Scenarios
    txs, expectations, scenario_counts = inject_fraud_scenarios(customers, devices, observation_start, seed, target_tx_count=cfg["transaction_count"])
    
    # 4. Defects
    txs, defect_counts = inject_quality_defects(txs, customers, devices, seed)
    
    write_csv(
        os.path.join(out_dir, "transactions.csv"),
        [
            "transaction_id", "customer_id", "device_id", "transaction_timestamp",
            "amount", "currency", "merchant_id", "merchant_category", "country", "status"
        ],
        txs
    )
    print(f"Generated {len(txs)} transactions -> transactions.csv")
    
    # 5. Manifests
    export_manifest(
        os.path.join(out_dir, "generator_manifest.json"),
        generator_version="1.0.0",
        seed=seed,
        generation_mode=args.mode,
        start_time=observation_start,
        end_time=observation_end,
        customer_count=len(customers),
        device_count=len(devices),
        transaction_count=len(txs),
        scenario_counts=scenario_counts,
        defect_counts=defect_counts
    )
    print("Exported generator_manifest.json")
    
    export_scenario_expected(
        os.path.join(out_dir, "scenario_expected.csv"),
        expectations
    )
    print("Exported scenario_expected.csv")
    print("=== Data Generation Complete ===")

if __name__ == "__main__":
    main()
