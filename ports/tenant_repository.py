"""
ITenantRepository port.
Abstracts customer/tenant database operations.
"""
from abc import ABC, abstractmethod

from domain.tenant.customer import Customer


class ITenantRepository(ABC):

    @abstractmethod
    def create_customer(self, customer: Customer) -> Customer:
        """Persist a new customer record. Returns saved customer."""

    @abstractmethod
    def provision_tenant_database(self, customer: Customer) -> None:
        """
        Create the isolated PostgreSQL database and all required schemas
        for a new customer. Delegates to TenantProvisioningService.
        """

    @abstractmethod
    def get_by_id(self, cust_id: str) -> Customer:
        """Return customer entity or raise CustomerNotFoundError."""
