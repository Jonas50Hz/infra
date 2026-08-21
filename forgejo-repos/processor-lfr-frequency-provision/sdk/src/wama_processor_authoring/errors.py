"""Errors raised when an authored processor contract is unsafe or invalid."""


class AuthoringValidationError(ValueError):
    """Raised when processor authoring evidence cannot be validated."""