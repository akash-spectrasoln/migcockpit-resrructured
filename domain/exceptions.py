"""
All domain exceptions.
Raised by use cases, caught by HTTP/adapter layer and translated to responses.
"""

class DomainError(Exception):
    """Base class for all domain exceptions."""
    pass

# Pipeline
class PipelineValidationError(DomainError):
    pass

class NodeConfigError(PipelineValidationError):
    pass

class CircularDependencyError(PipelineValidationError):
    pass

class UnsupportedNodeTypeError(PipelineValidationError):
    pass

class FilterPushdownError(DomainError):
    pass

# Connection
class EncryptionError(DomainError):
    pass

class ConnectionError(DomainError):
    pass

class CredentialDecryptionError(ConnectionError):
    pass

class UnsupportedSourceTypeError(ConnectionError):
    pass

# Job
class JobAlreadyRunningError(DomainError):
    pass

class JobNotFoundError(DomainError):
    pass

class InvalidStatusTransitionError(DomainError):
    pass

# Tenant
class TenantProvisioningError(DomainError):
    pass
