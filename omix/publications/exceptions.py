"""Custom exceptions for the publications subsystem."""

class InvalidAPIKeyError(Exception):
    """Raised when an API key is invalid or missing."""

    def __init__(self, source_name: str, message: str = "API key is invalid."):
        self.source_name = source_name
        super().__init__(f"[{source_name}] {message}")