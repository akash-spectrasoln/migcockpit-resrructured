"""
Customer domain entity (pure).
Database creation/provisioning logic lives in TenantProvisioningService.
Pure Python — zero framework imports.
"""
from dataclasses import dataclass


@dataclass
class Customer:
    cust_id: str
    name: str
    cust_db: str
    city: str
    region: str
    phone: str
    is_active: bool = True
