class AdapterError(Exception):
    """Base exception for adapter-related errors."""


class MissingCredentialsError(AdapterError):
    """Raised when required credentials are missing."""


class ExternalApiError(AdapterError):
    """Raised when external API request fails."""


class AdapterValidationError(AdapterError):
    """Raised when adapter response validation fails."""