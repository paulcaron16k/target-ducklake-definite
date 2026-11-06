"""Publishing writeCompleted events over NATS: optional, bounded, never fatal.

The ``nats`` setting holds the same keys as target-iceberg's, so one config
serves both targets. With ``nats.servers`` or ``nats.subject`` unset, nothing
here runs and the NATS client is never imported.

An event is a hint to a table maintainer, whose own schedule covers a lost
one; the rows are already committed when it is sent. So a failure to publish
(broker down, refused credentials, no TLS when ``tls`` is set, no answer)
logs one warning and the run still succeeds. Warnings never carry a
password, a token, or credentials embedded in a server URL.

The whole exchange, name resolution included, is bounded by
``PUBLISH_TIMEOUT_SECONDS``, and neither the run nor the interpreter's exit
waits longer. It runs on its own event loop in a daemon thread, which
``publish`` joins with the timeout. That alone is not enough: asyncio
resolves host names in its default executor, whose threads the interpreter
joins at exit, so a hung resolver would still hold the process. The loop
therefore resolves in daemon threads of its own (``_ResolvingLoop``). The
client still connects by host name, so TLS verifies the name and every
address a name resolves to is tried. Servers are tried in order, each with
its own connect timeout, until one connects.
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
from urllib.parse import SplitResult, urlsplit

from singer_sdk.exceptions import ConfigValidationError

if TYPE_CHECKING:
    import logging
    from collections.abc import Mapping, Sequence

    from nats.aio.client import Client

NATS_KEY = "nats"
# The whole exchange: resolve, connect, publish every event, flush, close.
PUBLISH_TIMEOUT_SECONDS = 10.0
# How much longer than the timeout publish waits for the loop to wind down
# before abandoning its thread.
_JOIN_GRACE_SECONDS = 1.0
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
    attempt = _Attempt(events, settings, timeout)
    thread = threading.Thread(target=attempt.run, name="nats-publish", daemon=True)
    thread.start()
    thread.join(timeout + _JOIN_GRACE_SECONDS)
    if thread.is_alive() or attempt.timed_out:
        reason = f"no answer within {timeout:g}s"
    elif attempt.transport is None:
        # Rebuilt from the messages, never chained: a client error can echo a
        # server URL with its credentials.
        first = (
            _describe(attempt.error)
            if attempt.error is not None
            else "the publish thread ended without publishing"
        )
        reason = settings.redact(
            "; ".join(dict.fromkeys([first, *attempt.server_errors]))
        )
    else:
        logger.info(
            "Published %d writeCompleted event(s) on %s over %s.",
            len(events),
            settings.subject,
            attempt.transport,
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


class _Attempt:
    """One publish, run on its own event loop in the thread ``publish`` starts.

    Attributes:
        transport: JetStream or core NATS, once published; None otherwise.
        error: Why it failed, if it did (other than the overall timeout).
        timed_out: Whether the overall timeout ended it.
        server_errors: Each server's reason for not being used, so a failure
            names every server's, not the last.
    """

    def __init__(
        self,
        events: Sequence[dict[str, Any]],
        settings: NatsSettings,
        timeout: float,
    ) -> None:
        self._events = events
        self._settings = settings
        self._timeout = timeout
        self.transport: str | None = None
        self.error: BaseException | None = None
        self.timed_out = False
        self.server_errors: list[str] = []

    def run(self) -> None:
        """Run the publish on a fresh loop, keeping its outcome.

        Any failure, building the loop included, is kept for ``publish``.
        The overall timeout cancels the publish and sets ``timed_out``, so it
        is told apart from a timeout inside it (a connection attempt's).
        """
        loop: _ResolvingLoop | None = None
        try:
            loop = _ResolvingLoop()
            # This thread's own loop, so code asking for the current one finds it.
            asyncio.set_event_loop(loop)
            task = loop.create_task(self._publish())

            def expire() -> None:
                self.timed_out = True
                task.cancel()

            loop.call_later(self._timeout, expire)
            self.transport = loop.run_until_complete(task)
        except BaseException as error:  # noqa: BLE001 (reported by publish)
            self.error = error
        finally:
            if loop is not None:
                _close(loop)

    async def _publish(self) -> str:
        """Connect to the first server that answers, publish, and close.

        Servers are tried in order, each with its own connect timeout, so a
        server that refuses, can't be reached, or accepts the connection but
        stays silent passes to the next, all within the overall timeout.

        Returns:
            The transport used: JetStream or core NATS.

        Raises:
            ConnectionError: If no server connected.
        """
        servers = list(self._settings.servers)
        if self._settings.tls:
            servers = await self._tls_servers(servers)
        for server in servers:
            connection = await self._connect(server)
            if connection is None:
                continue
            try:
                return await _send(connection, self._events, self._settings)
            finally:
                await connection.close()
        msg = "no server connected"
        raise ConnectionError(msg)

    async def _connect(self, server: str) -> Client | None:
        """Connect to one server, or record why not and return None."""
        import nats  # noqa: PLC0415 (only with events configured; an optional extra)

        reported: list[str] = []

        async def keep(error: Exception) -> None:
            reported.append(_describe(error))

        options = connect_options(self._settings)
        options["servers"] = [server]
        # nats-py reports the cause here, and raises a generic NoServersError.
        options["error_cb"] = keep
        try:
            return await nats.connect(**options)
        except Exception as error:  # noqa: BLE001 (named in the warning)
            causes = reported or [_describe(error)]
            self.server_errors.append(f"{server}: {'; '.join(dict.fromkeys(causes))}")
            return None

    async def _tls_servers(self, servers: list[str]) -> list[str]:
        """The servers that require TLS; with ``tls``, the others are left out.

        nats-py upgrades to TLS only when the server's INFO requires it, and
        sends CONNECT, credentials included, right after reading INFO, with
        no public hook between. So each server's INFO is read here first, on
        a connection of its own, and a server that does not require TLS
        never gets the client's. A broker with a tight connection cap or
        ``handshake_first`` TLS may answer that read with nothing; it then
        counts as not answering.

        Raises:
            PlaintextRefusedError: If no server requires TLS.
        """
        problems = {server: await _tls_problem(server) for server in servers}
        self.server_errors += [
            f"{server}: {problem}" for server, problem in problems.items() if problem
        ]
        secure = [server for server, problem in problems.items() if problem is None]
        if not secure:
            msg = (
                "tls is set and no server requires TLS; nothing was sent, "
                "rather than send it in plain text"
            )
            raise PlaintextRefusedError(msg)
        return secure


def _close(loop: asyncio.AbstractEventLoop) -> None:
    """Cancel what is left on the loop and close it.

    Not ``asyncio.run``: its ``shutdown_default_executor`` would wait for any
    executor thread, which is what this design avoids.
    """
    tasks = asyncio.all_tasks(loop)
    for task in tasks:
        task.cancel()
    if tasks:
        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
    loop.run_until_complete(loop.shutdown_asyncgens())
    asyncio.set_event_loop(None)
    loop.close()


def _describe(error: BaseException) -> str:
    """An error as ``Type: message``; a bare timeout says how long it waited.

    Every timeout inside the publish (connect, TLS check, flush, JetStream)
    is ``_CONNECT_TIMEOUT_SECONDS``; the overall one is reported apart.
    """
    if isinstance(error, (asyncio.TimeoutError, TimeoutError)) and not str(error):
        return f"{type(error).__name__}: no answer within {_CONNECT_TIMEOUT_SECONDS}s"
    return f"{type(error).__name__}: {error}"


class _ResolvingLoop(asyncio.SelectorEventLoop):
    """An event loop that resolves host names in daemon threads.

    The default resolver runs in the loop's executor, whose threads the
    interpreter joins at exit; a daemon thread is abandoned instead. Every
    address is returned, so connections try each in turn, as asyncio does.
    """

    async def getaddrinfo(  # noqa: PLR0913 (asyncio's signature)
        self,
        host: bytes | str | None,
        port: bytes | str | int | None,
        *,
        family: int = 0,
        type: int = 0,  # noqa: A002 (asyncio's name)
        proto: int = 0,
        flags: int = 0,
    ) -> list[Any]:
        """``socket.getaddrinfo``, in a daemon thread."""
        result: concurrent.futures.Future[list[Any]] = concurrent.futures.Future()

        def resolve() -> None:
            if not result.set_running_or_notify_cancel():
                return
            try:
                result.set_result(
                    socket.getaddrinfo(host, port, family, type, proto, flags)
                )
            except Exception as error:  # noqa: BLE001 (handed to the awaiting side)
                result.set_exception(error)

        threading.Thread(target=resolve, name="nats-resolve", daemon=True).start()
        return await asyncio.wrap_future(result)


async def _send(
    connection: Client, events: Sequence[dict[str, Any]], settings: NatsSettings
) -> str:
    """Publish every event on the connection.

    Returns:
        The transport used: JetStream or core NATS.
    """
    if await _uses_jetstream(connection, settings):
        stream = connection.jetstream(timeout=_CONNECT_TIMEOUT_SECONDS)
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


async def _tls_problem(server: str) -> str | None:
    """Why the server can't be used with ``tls``, or None if it requires TLS."""
    try:
        await _require_tls(server)
    except Exception as error:  # noqa: BLE001 (named in the warning)
        return f"{type(error).__name__}: {error}"
    return None


async def _require_tls(server: str) -> None:
    """Read the server's INFO, and refuse a server that doesn't require TLS.

    Raises:
        PlaintextRefusedError: If the server does not require TLS.
    """
    parts = _split(server)
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(parts.hostname, parts.port or _DEFAULT_PORT),
        _CONNECT_TIMEOUT_SECONDS,
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
            "the server does not require TLS (it may offer it as optional, "
            "which nats-py never uses)"
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


def connect_options(settings: NatsSettings) -> dict[str, Any]:
    """``nats.connect`` options, without the servers: one quick attempt each."""
    options: dict[str, Any] = {
        "name": "target-ducklake",
        "connect_timeout": _CONNECT_TIMEOUT_SECONDS,
        # nats-py retries the first connection too unless all three are set.
        "allow_reconnect": False,
        "max_reconnect_attempts": 1,
        "reconnect_time_wait": 0,
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
