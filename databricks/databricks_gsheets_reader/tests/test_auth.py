"""Tests for authentication module."""

import json
import pytest
from databricks_gsheets_reader.auth import GoogleAuth


class TestGoogleAuthInit:
    """Test GoogleAuth initialization."""

    def test_init_with_credentials_json(self):
        """Should accept credentials_json parameter."""
        fake_creds = json.dumps({
            "type": "service_account",
            "project_id": "test-project",
            "private_key_id": "key123",
            "private_key": "FAKE_PRIVATE_KEY_FOR_TESTING",  # gitleaks:allow
            "client_email": "test@test-project.iam.gserviceaccount.com",
            "client_id": "123456789",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        })
        auth = GoogleAuth(credentials_json=fake_creds)
        assert auth._credentials_json == fake_creds
        assert auth._secret_scope is None
        assert auth._secret_key is None

    def test_init_with_secret_scope_and_key(self):
        """Should accept secret_scope and secret_key parameters."""
        auth = GoogleAuth(secret_scope="my-scope", secret_key="my-key")
        assert auth._secret_scope == "my-scope"
        assert auth._secret_key == "my-key"
        assert auth._credentials_json is None

    def test_init_requires_one_auth_method(self):
        """Should raise ValueError if no auth method provided."""
        with pytest.raises(ValueError, match="Must provide either"):
            GoogleAuth()

    def test_init_rejects_both_auth_methods(self):
        """Should raise ValueError if both auth methods provided."""
        with pytest.raises(ValueError, match="Cannot provide both"):
            GoogleAuth(
                credentials_json='{"type": "service_account"}',
                secret_scope="scope",
                secret_key="key"
            )

    def test_init_requires_both_secret_params(self):
        """Should raise ValueError if only one secret param provided."""
        with pytest.raises(ValueError, match="Both secret_scope and secret_key"):
            GoogleAuth(secret_scope="scope")
        with pytest.raises(ValueError, match="Both secret_scope and secret_key"):
            GoogleAuth(secret_key="key")
