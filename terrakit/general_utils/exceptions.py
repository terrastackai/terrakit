# © Copyright IBM Corporation 2025
# SPDX-License-Identifier: Apache-2.0


from typing import Any, Dict, Union


class TerrakitBaseException(Exception):
    """Base exception for all custom exceptions in the project."""

    def __init__(self, message: str, details: Union[None, str, Dict[Any, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} - Additional details: {self.details}"
        return self.message


class TerrakitValidationError(TerrakitBaseException):
    """Raised when there is an validation error."""

    pass


class TerrakitValueError(TerrakitBaseException):
    """Raised when there is invalid input."""

    pass


class TerrakitMissingEnvironmentVariable(TerrakitBaseException):
    pass
