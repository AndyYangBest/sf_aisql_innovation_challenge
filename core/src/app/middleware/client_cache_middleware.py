from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint


class ClientCacheMiddleware(BaseHTTPMiddleware):
    """Middleware to set the `Cache-Control` header for client-side caching on all responses.

    Parameters
    ----------
    app: FastAPI
        The FastAPI application instance.
    max_age: int, optional
        Duration (in seconds) for which the response should be cached. Defaults to 60 seconds.

    Attributes
    ----------
    max_age: int
        Duration (in seconds) for which the response should be cached.

    Methods
    -------
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        Process the request and set the `Cache-Control` header in the response.

    Note
    ----
        - The `Cache-Control` header instructs clients (e.g., browsers)
        to cache the response for the specified duration.
    """

    def __init__(self, app: FastAPI, max_age: int = 60) -> None:
        super().__init__(app)
        self.max_age = max_age

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Process the request and set the `Cache-Control` header in the response.

        Parameters
        ----------
        request: Request
            The incoming request.
        call_next: RequestResponseEndpoint
            The next middleware or route handler in the processing chain.

        Returns
        -------
        Response
            The response object with the `Cache-Control` header set.

        Note
        ----
            - This method is automatically called by Starlette for processing the request-response cycle.
        """
        response: Response = await call_next(request)
        path = request.url.path
        request_cache = (request.headers.get("cache-control") or "").lower()
        no_store_paths = (
            "/api/v1/column-metadata",
            "/api/v1/column-workflows",
            "/api/v1/table-assets",
        )

        if request.method != "GET" or "no-store" in request_cache or "no-cache" in request_cache:
            response.headers["Cache-Control"] = "no-store"
            return response

        if any(path.startswith(prefix) for prefix in no_store_paths):
            response.headers["Cache-Control"] = "no-store"
            return response

        if "cache-control" not in {key.lower() for key in response.headers.keys()}:
            response.headers["Cache-Control"] = f"public, max-age={self.max_age}"

        return response
