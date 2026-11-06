"""Publishing writeCompleted events over NATS: optional, bounded, never fatal.

The ``nats`` setting holds the same keys as target-iceberg's, so one config
serves both targets. With ``nats.servers`` or ``nats.subject`` unset, nothing
here runs and the NATS client is never imported.

An event is a hint to a table maintainer, whose own schedule covers a lost
one; the rows are already committed when it is sent. So a failure to publish
(broker down, refused credentials, no answer) logs one warning and the run
still succeeds, and the whole exchange is bounded by
``PUBLISH_TIMEOUT_SECONDS``. Warnings never carry a password, a token, or
credentials embedded in a server URL.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import ssl
from typing import TYPE_CHECKING, Any, NamedTuple
from urllib.parse import urlsplit

if TYPE_CHECKING:
    import logging
    from collections.abc import Mapping, Sequence

    from nats.aio.client import Client

NATS_KEY = "nats"
# The whole exchange: connect, publish every event, flush, close.
PUBLISH_TIMEOUT_SECONDS = 10.0
# Each connection attempt, and auto's JetStream probe.
_CONNECT_TIMEOUT_SECONDS = 2
JETSTREAM_MODES = ("auto", "true", "false")
REDACTED = "***"


class NatsError(RuntimeError):
    """Events are configured, but the NATS client is not installed."""


class NatsSettings(NamedTuple):
    """The ``nats`` setting, resolved.

    Attributes:
        servers: The server URLs.
        subject: The subject events are published on.
        jetstream: ``auto`` (JetStream when a stream covers the subject),
            ``true`` or ``false``.
        username: For user/password authentication.
        password: For user/password authentication.
        token: Token authentication; wins over user/password.
        creds_file: A NATS ``.creds`` file; wins over both.
        tls: Whether to connect over TLS, trusting the system's CAs.
    """

    servers: tuple[str, ...]
    subject: str
    jetstream: str = "auto"
    username: str | None = None
    password: str | None = None
    token: str | None = None
    creds_file: str | None = None
    tls: bool = False

    def secrets(self) -> list[str]:
        """Every credential a message could echo, longest first."""
        found = [self.password, self.token]
        for server in self.servers:
            parts = urlsplit(server if "://" in server else f"nats://{server}")
            found += [parts.username, parts.password]
        return sorted({secret for secret in found if secret}, key=len, reverse=True)

    def redact(self, text: str) -> str:
        """``text`` with every credential replaced by ``***``."""
        for secret in self.secrets():
            text = text.replace(secret, REDACTED)
        return text


def nats_settings(config: Mapping[str, Any]) -> NatsSettings | None:
    """The ``nats`` setting, or None unless both servers and a subject are set.

    The config schema has already checked the values' types and
    ``jetstream``'s value.

    Args:
        config: The target's config.

    Returns:
        The settings, or None when events are off.
    """
    block = config.get(NATS_KEY) or {}
    servers = tuple(
        server.strip()
        for server in (block.get("servers") or "").split(",")
        if server.strip()
    )
    subject = (block.get("subject") or "").strip()
    if not servers or not subject:
        return None
    return NatsSettings(
        servers=servers,
        subject=subject,
        jetstream=block.get("jetstream") or "auto",
        username=block.get("username") or None,
        password=block.get("password") or None,
        token=block.get("token") or None,
        creds_file=block.get("creds_file") or None,
        tls=bool(block.get("tls")),
    )


def check_nats_config(config: Mapping[str, Any]) -> NatsSettings | None:
    """Resolve the ``nats`` setting at startup, and check the client is installed.

    Args:
        config: The target's config.

    Returns:
        The settings, or None when events are off.

    Raises:
        NatsError: If events are configured but nats-py is not installed.
    """
    settings = nats_settings(config)
    if settings is not None and importlib.util.find_spec("nats") is None:
        msg = (
            "nats.servers and nats.subject are set, so writeCompleted events "
            "are published over NATS, but the NATS client is not installed. "
            "Install target-ducklake with the nats extra "
            "(target-ducklake[nats]), or unset nats.servers."
        )
        raise NatsError(msg)
    return settings


def publish(
    events: Sequence[dict[str, Any]],
    settings: NatsSettings,
    logger: logging.Logger,
    *,
    timeout: float = PUBLISH_TIMEOUT_SECONDS,
) -> bool:
    """Publish the events; on any failure, warn and return False, never raise.

    Args:
        events: The events, one per table.
        settings: Where and how to publish.
        logger: Where the outcome is logged.
        timeout: Seconds the whole exchange may take.

    Returns:
        Whether every event was published.
    """
    if not events:
        return True
    try:
        transport = asyncio.run(
            asyncio.wait_for(_publish(events, settings), timeout=timeout)
        )
    except (asyncio.TimeoutError, TimeoutError):
        reason = f"no answer within {timeout:g}s"
    except Exception as error:  # noqa: BLE001 (advisory: never fails the run)
        # Rebuilt from the message, never chained: a client error can echo a
        # server URL with its credentials.
        reason = settings.redact(f"{type(error).__name__}: {error}")
    else:
        logger.info(
            "Published %d writeCompleted event(s) on %s over %s.",
            len(events),
            settings.subject,
            transport,
        )
        return True
    logger.warning(
        "writeCompleted events were not published (%s) to %s on %s. The "
        "run's data is committed and unaffected; a table maintainer falls "
        "back to its schedule.",
        reason,
        ", ".join(settings.redact(server) for server in settings.servers),
        settings.subject,
    )
    return False


async def _publish(events: Sequence[dict[str, Any]], settings: NatsSettings) -> str:
    """Connect, publish every event, and close.

    Returns:
        The transport used: JetStream or core NATS.
    """
    import nats  # noqa: PLC0415 (only with events configured; an optional extra)

    connection = await nats.connect(**connect_options(settings))
    try:
        if await _uses_jetstream(connection, settings):
            stream = connection.jetstream()
            for event in events:
                # The deterministic id lets the server drop a re-published
                # event within its duplicate window.
                await stream.publish(
                    settings.subject,
                    _encode(event),
                    headers={"Nats-Msg-Id": event["id"]},
                )
            return "JetStream"
        for event in events:
            await connection.publish(settings.subject, _encode(event))
        # Core NATS buffers in the client: flush before closing, or the
        # events may never leave.
        await connection.flush(timeout=_CONNECT_TIMEOUT_SECONDS)
        return "core NATS"
    finally:
        await connection.close()


async def _uses_jetstream(connection: Client, settings: NatsSettings) -> bool:
    """Whether to publish through JetStream.

    ``auto`` uses it when a stream covers the subject, which also tells that
    JetStream is enabled for this account; otherwise core NATS, as a
    JetStream publish with no stream would fail.
    """
    if settings.jetstream != "auto":
        return settings.jetstream == "true"
    try:
        await connection.jetstream(
            timeout=_CONNECT_TIMEOUT_SECONDS
        ).find_stream_name_by_subject(settings.subject)
    except Exception:  # noqa: BLE001 (no JetStream, or no stream: core NATS)
        return False
    return True


async def _ignore_client_error(_error: Exception) -> None:
    """Drop the client's own error reports; ``publish`` reports, redacted."""


def connect_options(settings: NatsSettings) -> dict[str, Any]:
    """``nats.connect`` options: one quick attempt per server, then give up."""
    options: dict[str, Any] = {
        "servers": list(settings.servers),
        "name": "target-ducklake",
        "connect_timeout": _CONNECT_TIMEOUT_SECONDS,
        # nats-py retries the first connection too unless all three are set.
        "allow_reconnect": False,
        "max_reconnect_attempts": 1,
        "reconnect_time_wait": 0,
        "error_cb": _ignore_client_error,
    }
    if settings.creds_file:
        options["user_credentials"] = settings.creds_file
    elif settings.token:
        options["token"] = settings.token
    elif settings.username:
        options["user"] = settings.username
        options["password"] = settings.password
    if settings.tls:
        options["tls"] = ssl.create_default_context()
    return options


def _encode(event: dict[str, Any]) -> bytes:
    """The event as structured-mode CloudEvents JSON."""
    return json.dumps(event, separators=(",", ":")).encode()
