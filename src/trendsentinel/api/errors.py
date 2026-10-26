"""Consistent error handling for the HTTP API.

Every error response uses one envelope, so a client has a single shape to parse:

    {"error": {"code": "...", "message": "...", "details": ...}}

Internal failures are logged in full and reported generically: a client must never see a
stack trace, SQL, or a connection string.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from trendsentinel.logging import get_logger

__all__ = ["ApiError", "ErrorCode", "register_exception_handlers"]

logger = get_logger(__name__)


class ErrorCode:
    """Stable, machine-readable error codes.

    Clients may branch on these; they are part of the API contract and must not change
    silently.
    """

    BAD_REQUEST = "bad_request"
    UNAUTHORIZED = "unauthorized"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    VALIDATION_ERROR = "validation_error"
    SERVICE_UNAVAILABLE = "service_unavailable"
    INTERNAL_ERROR = "internal_error"


class ApiError(Exception):
    """An error that maps directly onto the response envelope."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 400,
        details: Any = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details

    def to_response(self) -> JSONResponse:
        """Render this error as the standard envelope."""
        return JSONResponse(
            status_code=self.status_code,
            content={
                "error": {
                    "code": self.code,
                    "message": self.message,
                    "details": self.details,
                }
            },
        )


def envelope(code: str, message: str, details: Any = None) -> dict[str, Any]:
    """Build the error body for handlers that cannot use :class:`ApiError`."""
    return {"error": {"code": code, "message": message, "details": details}}


def register_exception_handlers(app: FastAPI) -> None:
    """Attach the handlers that guarantee one error shape across the API."""

    @app.exception_handler(ApiError)
    async def handle_api_error(_request: Request, exc: ApiError) -> JSONResponse:
        return exc.to_response()

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # The detail is FastAPI's field-by-field report; it names parameters, never internals.
        return JSONResponse(
            status_code=422,
            content=envelope(
                ErrorCode.VALIDATION_ERROR,
                "Request validation failed",
                details=_clean_validation_errors(exc),
            ),
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_exception(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {
            401: ErrorCode.UNAUTHORIZED,
            403: ErrorCode.UNAUTHORIZED,
            404: ErrorCode.NOT_FOUND,
            409: ErrorCode.CONFLICT,
            503: ErrorCode.SERVICE_UNAVAILABLE,
        }.get(exc.status_code, ErrorCode.BAD_REQUEST)

        # `exc.detail` is a developer-controlled string here, never a traceback.
        return JSONResponse(
            status_code=exc.status_code,
            content=envelope(code, str(exc.detail)),
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        # The detail goes to the log; the client gets a generic message and nothing else.
        logger.exception(
            "unhandled error",
            extra={"path": request.url.path, "method": request.method},
        )
        return JSONResponse(
            status_code=500,
            content=envelope(ErrorCode.INTERNAL_ERROR, "Internal server error"),
        )


def _clean_validation_errors(exc: RequestValidationError) -> list[dict[str, Any]]:
    """Reduce FastAPI's validation report to fields safe to return.

    The raw report can contain the offending input value, which may be a credential, so the
    input is dropped and only the location and message are kept.
    """
    cleaned: list[dict[str, Any]] = []
    for error in exc.errors():
        cleaned.append(
            {
                "location": [str(part) for part in error.get("loc", ())],
                "message": str(error.get("msg", "invalid value")),
                "type": str(error.get("type", "value_error")),
            }
        )

    return cleaned
