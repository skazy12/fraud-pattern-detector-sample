"""
Unit tests for synthetic data generator.
"""
import pytest
from src.customers import generate_customers
from src.devices import generate_devices
from src.scenarios import inject_fraud_scenarios
from src.defects import inject_quality_defects
from datetime import datetime

def test_reproducible_customer_generation():
    t0 = datetime(2026, 1, 1, 0, 0, 0)
    c1 = generate_customers(10, 42, t0)
    c2 = generate_customers(10, 42, t0)
    assert len(c1) == 10
    assert c1 == c2

def test_reproducible_device_generation():
    t0 = datetime(2026, 1, 1, 0, 0, 0)
    customers = generate_customers(10, 42, t0)
    d1 = generate_devices(customers, 15, 42)
    d2 = generate_devices(customers, 15, 42)
    assert len(d1) == 15
    assert d1 == d2

def test_scenario_injection():
    t0 = datetime(2026, 1, 1, 0, 0, 0)
    customers = generate_customers(10, 42, t0)
    devices = generate_devices(customers, 15, 42)
    txs, exp, counts = inject_fraud_scenarios(customers, devices, t0, 42)
    
    assert counts["R001"] == 1
    assert counts["R002"] == 1
    assert counts["R003"] == 1
    assert counts["R004"] == 1
    assert counts["R005"] == 1
    assert len(exp) == len(txs)
