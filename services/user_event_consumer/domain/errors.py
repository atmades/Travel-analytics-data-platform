class ConsumerError(Exception):
    """Base consumer error."""

class InfrastructureError(ConsumerError):
    """Temporary infrastructure failure. Should be retried without offset commit."""


# class DataValidationError(ConsumerError):
#     """Invalid event payload. Should be routed to DLQ."""


# class DLQWriteError(ConsumerError):
#     """Failed to write event to DLQ."""

