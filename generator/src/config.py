"""
Configuration and catalogs for synthetic transaction generation.
"""

# Catalogs according to data_dictionary.md
TRANSACTION_STATUSES = ["APPROVED", "DECLINED"]
RISK_PROFILES = ["LOW", "MEDIUM", "HIGH"]
DEVICE_TYPES = ["MOBILE", "DESKTOP", "TABLET"]
MERCHANT_CATEGORIES = [
    "RETAIL", "ONLINE", "TRAVEL", "FOOD", "ENTERTAINMENT", "SERVICES"
]
SUPPORTED_COUNTRIES = ["ES", "FR", "PT", "DE", "IT", "GB", "US", "BO"]

# Thresholds for R002 High Amount
HIGH_AMOUNT_THRESHOLDS = {
    "LOW": 2000.00,
    "MEDIUM": 1500.00,
    "HIGH": 1000.00
}

# Mode specifications
MODE_CONFIGS = {
    "controlled": {
        "customer_count": 10,
        "device_count": 15,
        "transaction_count": 100,
        "inject_scenarios": True,
        "inject_defects": True
    },
    "dev": {
        "customer_count": 100,
        "device_count": 150,
        "transaction_count": 1000,
        "inject_scenarios": True,
        "inject_defects": True
    },
    "full": {
        "customer_count": 1000,
        "device_count": 1500,
        "transaction_count": 50000,
        "inject_scenarios": True,
        "inject_defects": True
    }
}
