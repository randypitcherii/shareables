"""Authentication handling for Google Sheets API."""

from __future__ import annotations

import base64
import json
import threading
from typing import TYPE_CHECKING

from google.oauth2 import service_account
import google.auth.transport.requests

if TYPE_CHECKING:
    from google.oauth2.service_account import Credentials


class GoogleAuth:
    """Handles Google Sheets API authentication.

    Supports two authentication methods:
    1. Direct credentials JSON string
    2. Databricks secret scope/key reference (resolved lazily)
    """

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    def __init__(
        self,
        *,
        credentials_json: str | None = None,
        secret_scope: str | None = None,
        secret_key: str | None = None,
    ) -> None:
        """Initialize authentication.

        Args:
            credentials_json: GCP service account JSON string
            secret_scope: Databricks secret scope name
            secret_key: Databricks secret key name

        Raises:
            ValueError: If auth configuration is invalid
        """
        has_json = credentials_json is not None
        has_scope = secret_scope is not None
        has_key = secret_key is not None

        if has_json and (has_scope or has_key):
            raise ValueError(
                "Cannot provide both credentials_json and secret_scope/secret_key"
            )

        if not has_json and not (has_scope and has_key):
            if has_scope or has_key:
                raise ValueError(
                    "Both secret_scope and secret_key must be provided together"
                )
            raise ValueError(
                "Must provide either credentials_json or (secret_scope and secret_key)"
            )

        self._credentials_json = credentials_json
        self._secret_scope = secret_scope
        self._secret_key = secret_key
        self._cached_credentials: Credentials | None = None
        self._lock = threading.Lock()

    def get_credentials(self) -> Credentials:
        """Get Google credentials, resolving from Databricks secrets if needed.

        Thread-safe with double-checked locking.

        Returns:
            Google service account credentials

        Raises:
            ValueError: If credentials cannot be loaded
        """
        # Fast path without lock
        if self._cached_credentials is not None:
            return self._cached_credentials

        # Acquire lock for initialization
        with self._lock:
            # Double-check after acquiring lock
            if self._cached_credentials is not None:
                return self._cached_credentials

            if self._credentials_json:
                json_content = self._credentials_json
            else:
                json_content = self._load_from_databricks_secret()

            try:
                service_account_info = json.loads(json_content)
                self._cached_credentials = (
                    service_account.Credentials.from_service_account_info(
                        service_account_info,
                        scopes=self.SCOPES,
                    )
                )
                return self._cached_credentials
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in credentials: {e}") from e
            except Exception as e:
                raise ValueError(f"Failed to create credentials: {e}") from e

    def _load_from_databricks_secret(self) -> str:
        """Load credentials JSON from Databricks secret.

        Returns:
            Decoded JSON string from secret
        """
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        secret_response = w.secrets.get_secret(
            scope=self._secret_scope,
            key=self._secret_key,
        )
        # Databricks secrets are base64 encoded
        return base64.b64decode(secret_response.value).decode("utf-8")

    def get_access_token(self) -> str:
        """Get a valid access token, refreshing if needed.

        Thread-safe with double-checked locking.

        Returns:
            OAuth2 access token string
        """
        creds = self.get_credentials()

        # Fast path without lock if token is valid
        if creds.valid:
            return creds.token

        # Acquire lock for token refresh
        with self._lock:
            # Double-check after acquiring lock
            if not creds.valid:
                auth_req = google.auth.transport.requests.Request()
                creds.refresh(auth_req)
            return creds.token
