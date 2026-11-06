"""Publishing writeCompleted events over NATS: optional, bounded, never fatal.

The ``nats`` setting holds the same keys as target-iceberg's, so one config
serves both targets. With ``nats.servers`` or ``nats.subject`` unset, nothing
here runs and the NATS client is never imported.

An event is a hint to a table maintainer, whose own schedule covers a lost
one; the rows are already committed when it is sent. So a failure to publish
(broker down, refused credentials, no TLS when ``tls`` is set, no answer)
logs one warning and the run still succeeds. The whole exchange, name
resolution included, is bounded by ``PUBLISH_TIMEOUT_SECONDS``: host names
are resolved in daemon threads, which neither the event loop nor the
interpreter waits for, and the client is given the address. Warnings never
carry a password, a token, or credentials embedded in a server URL.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import dataclasses
import importlib.util
import json
import socket
import ssl
import threading
from typing import TYPE_CHECKING, Any
from urllib.parse import SplitResult, urlsplit, urlunsplit

from singer_sdk.exceptions import ConfigValidationError

if TYPE_CHECKING:
    import logging
    from collections.abc import Mapping, Sequence
    from types import ModuleType

    from nats.aio.client import Client

NATS_KEY = "nats"
# The whole exchange: resolve, connect, publish every event, flush, close.
PUBLISH_TIMEOUT_SECONDS = 10.0
# Each connection attempt, the TLS check, and auto's JetStream probe.
_CONNECT_TIMEOUT_SECONDS = 2
_DEFAULT_PORT = 4222
JETSTREAM_MODES = ("auto", "true", "false")
REDACTED = "***"


class PlaintextRefusedError(ConnectionError):
    """``tls`` is set, but the server does not require TLS: nothing is sent."""


@dataclasses.dataclass(frozen=True, repr=False)
class NatsSettings:
    """The ``nats`` setting, resolved.

    Its ``repr`` is redacted, so a log or traceback never shows a credential.

    Attributes:
        servers: The server URLs.
        subject: The subject events are published on.
        jetstream: ``auto`` (JetStream when a stream covers the subject),
            ``true`` or ``false``.
        username: For user/password authentication.
        password: For user/password authentication.
        token: Token authentication; wins over user/password.
        creds_file: A NATS ``.creds`` file; wins over both.
        tls: Connect over TLS only, trusting the system's CAs.
    """

    servers: tuple[str, ...]
    subject: str
    jetstream: str = "auto"
    username: str | None = None
    password: str | None = None
    token: str | None = None
    creds_file: str | None = None
    tls: bool = False

    def __repr__(self) -> str:
        """The settings, with every credential replaced by ``***``."""
        fields = ", ".join(
            f"{field.name}={getattr(self, field.name)!r}"
            for field in dataclasses.fields(self)
        )
        return self.redact(f"NatsSettings({fields})")

    def secrets(self) -> list[str]:
        """Every credential a message could echo, longest first."""
        found = [self.password, self.token]
        for server in self.servers:
            parts = _split(server)
            found += [parts.username, parts.password]
        return sorted({secret for secret in found if secret}, key=len, reverse=True)

    def redact(self, text: str) -> str:
        """``text`` with every credential replaced by ``***``."""
        return redact(text, self.secrets())


def redact(text: str, secrets: Sequence[str]) -> str:
    """``text`` with each of ``secrets`` replaced by ``***``, longest first."""
    for secret in sorted(secrets, key=len, reverse=True):
        if secret:
            text = text.replace(secret, REDACTED)
    return text


def _split(server: str) -> SplitResult:
    """A server URL's parts; a bare ``host:port`` is a ``nats://`` URL."""
    return urlsplit(server if "://" in server else f"nats://{server}")


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


def check_nats_config(
    config: Mapping[str, Any], logger: logging.Logger
) -> NatsSettings | None:
    """Resolve the ``nats`` setting at startup, and check the client is installed.

    Only one of ``servers`` and ``subject`` leaves events off, as in
    target-iceberg, with a warning naming the missing one.

    Args:
        config: The target's config.
        logger: Where the warning goes.

    Returns:
        The settings, or None when events are off.

    Raises:
        ConfigValidationError: If events are configured but the NATS client
            is not installed.
    """
    settings = nats_settings(config)
    if settings is None:
        block = config.get(NATS_KEY) or {}
        given = [
            key for key in ("servers", "subject") if (block.get(key) or "").strip()
        ]
        if len(given) == 1:
            missing = "subject" if given == ["servers"] else "servers"
            logger.warning(
                "nats.%s is set but nats.%s is not: writeCompleted events are "
                "off. Set both to publish them.",
                given[0],
                missing,
            )
        return None
    if importlib.util.find_spec("nats") is None:
        msg = (
            "nats.servers and nats.subject are set, so writeCompleted events "
            "are published over NATS, but the NATS client is not installed. "
            "Install target-ducklake with the nats extra "
            "(target-ducklake[nats]), or unset nats.servers."
        )
        raise ConfigValidationError(msg)
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
    """Connect to the first server that answers, publish every event, and close.

    Returns:
        The transport used: JetStream or core NATS.

    Raises:
        Exception: The last server's connection error, when none connects.
    """
    import nats  # noqa: PLC0415 (only with events configured; an optional extra)

    errors: list[Exception] = []
    for server in settings.servers:
        try:
            connection = await _connect(nats, server, settings)
        except Exception as error:  # noqa: BLE001 (try the next server)
            errors.append(error)
            continue
        try:
            return await _send(connection, events, settings)
        finally:
            await connection.close()
    raise errors[-1]


async def _send(
    connection: Client, events: Sequence[dict[str, Any]], settings: NatsSettings
) -> str:
    """Publish every event on the connection.

    Returns:
        The transport used: JetStream or core NATS.
    """
    if await _uses_jetstream(connection, settings):
        stream = connection.jetstream()
        for event in events:
            # The deterministic id lets the server drop a re-published event
            # within its duplicate window.
            await stream.publish(
                settings.subject,
                _encode(event),
                headers={"Nats-Msg-Id": event["id"]},
            )
        return "JetStream"
    for event in events:
        await connection.publish(settings.subject, _encode(event))
    # Core NATS buffers in the client: flush before closing, or the events may
    # never leave.
    await connection.flush(timeout=_CONNECT_TIMEOUT_SECONDS)
    return "core NATS"


async def _connect(nats: ModuleType, server: str, settings: NatsSettings) -> Client:
    """Connect to one server, by the address its host name resolves to.

    With ``tls``, the server must require TLS before the client sends
    anything: nats-py upgrades only when the server asks, so a server that
    doesn't would otherwise get the credentials in plain text.

    Raises:
        PlaintextRefusedError: With ``tls``, if the server does not require TLS.
    """
    parts = _split(server)
    host = parts.hostname or "localhost"
    port = parts.port or _DEFAULT_PORT
    address = await _resolve(host, port)
    if settings.tls:
        await _require_tls(address, port)
    shown = f"[{address}]" if ":" in address else address
    userinfo = parts.netloc.rpartition("@")[0]
    netloc = f"{userinfo}@{shown}:{port}" if userinfo else f"{shown}:{port}"
    options = connect_options(settings)
    options["servers"] = [urlunsplit((parts.scheme, netloc, "", "", ""))]
    if settings.tls:
        # Verify the certificate against the host name, not the address.
        options["tls_hostname"] = host
    return await nats.connect(**options)


async def _resolve(host: str, port: int) -> str:
    """The first address of ``host``, resolved in a daemon thread.

    The event loop's resolver runs in its default executor, whose threads
    ``asyncio.run`` and the interpreter both wait for at exit: a resolver
    that hangs would outlast any timeout. A daemon thread is abandoned.
    """
    result: concurrent.futures.Future[str] = concurrent.futures.Future()

    def resolve() -> None:
        if not result.set_running_or_notify_cancel():
            return
        try:
            infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            result.set_result(str(infos[0][4][0]))
        except Exception as error:  # noqa: BLE001 (handed to the awaiting side)
            result.set_exception(error)

    threading.Thread(target=resolve, name="nats-resolve", daemon=True).start()
    return await asyncio.wrap_future(result)


async def _require_tls(address: str, port: int) -> None:
    """Read the server's INFO, and refuse a server that doesn't require TLS.

    Raises:
        PlaintextRefusedError: If the server does not require TLS.
    """
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(address, port), _CONNECT_TIMEOUT_SECONDS
    )
    try:
        line = await asyncio.wait_for(reader.readline(), _CONNECT_TIMEOUT_SECONDS)
    finally:
        writer.close()
    _, _, info = line.decode(errors="replace").partition("INFO ")
    try:
        required = bool(json.loads(info).get("tls_required"))
    except ValueError:
        required = False
    if not required:
        msg = (
            "tls is set but the server does not require TLS; nothing was "
            "sent, rather than send it in plain text"
        )
        raise PlaintextRefusedError(msg)


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
    """``nats.connect`` options, without the server: one quick attempt, no retry."""
    options: dict[str, Any] = {
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
