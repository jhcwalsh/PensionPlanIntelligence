"""Cover the GitHub workflow_dispatch helper used by the approval flow."""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest
import requests

from insights import github_dispatch


def test_is_configured_false_without_token(monkeypatch):
    monkeypatch.delenv("GITHUB_DISPATCH_TOKEN", raising=False)
    assert github_dispatch.is_configured() is False


def test_is_configured_true_with_token(monkeypatch):
    monkeypatch.setenv("GITHUB_DISPATCH_TOKEN", "ghp_test")
    assert github_dispatch.is_configured() is True


def test_dispatch_raises_when_token_missing(monkeypatch):
    monkeypatch.delenv("GITHUB_DISPATCH_TOKEN", raising=False)
    with pytest.raises(github_dispatch.DispatchError, match="not set"):
        github_dispatch.dispatch_publish_approved(13)


def test_dispatch_posts_to_correct_endpoint(monkeypatch):
    monkeypatch.setenv("GITHUB_DISPATCH_TOKEN", "ghp_test")
    resp = MagicMock(status_code=204)
    with patch.object(github_dispatch.requests, "post", return_value=resp) as mock_post:
        github_dispatch.dispatch_publish_approved(13)
    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert args[0].endswith("/publish-approved.yml/dispatches")
    assert kwargs["headers"]["Authorization"] == "Bearer ghp_test"
    assert kwargs["json"]["ref"] == "master"
    assert kwargs["json"]["inputs"]["publication_id"] == "13"


def test_dispatch_raises_on_http_error(monkeypatch):
    monkeypatch.setenv("GITHUB_DISPATCH_TOKEN", "ghp_test")
    resp = MagicMock(status_code=401)
    resp.text = "Bad credentials"
    with patch.object(github_dispatch.requests, "post", return_value=resp):
        with pytest.raises(github_dispatch.DispatchError, match="HTTP 401"):
            github_dispatch.dispatch_publish_approved(13)


def test_dispatch_raises_on_network_error(monkeypatch):
    monkeypatch.setenv("GITHUB_DISPATCH_TOKEN", "ghp_test")
    with patch.object(
        github_dispatch.requests, "post",
        side_effect=requests.ConnectionError("boom"),
    ):
        with pytest.raises(github_dispatch.DispatchError, match="request failed"):
            github_dispatch.dispatch_publish_approved(13)
