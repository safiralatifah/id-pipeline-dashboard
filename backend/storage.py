"""Object storage for a Substrait app — the same code locally and deployed.

Declare it first; the manifest is the only trigger:

    # substrait.yaml
    services:
      object-storage: {}

Deployed: the platform injects OBJECT_STORAGE_BUCKET and gives the pod its own Google Cloud
identity. There is no key, no secret and nothing for you to configure.
Locally:  `docker compose up -d gcs` runs fake-gcs-server; export the two vars from
          docker-compose.yml's comment block and this module talks to it instead.

Nothing here runs until you call it: the SDK import is lazy, so an app that never touches
storage neither needs the dependency nor pays for it at startup.

Every function here is SYNCHRONOUS and blocking — it makes an HTTP call, and on a 403 it
also sleeps between retries. Call them from a plain `def` handler (FastAPI already runs
those in a threadpool), or, from an `async def` one, hop out of the event loop:

    from starlette.concurrency import run_in_threadpool
    url = await run_in_threadpool(storage.download_url, key, expires_seconds=900)

Calling one directly inside `async def` stalls every other request the worker is serving —
including /health — for as long as the call takes.

Provider note: this module is the ONLY place that knows about Google Cloud Storage. If the
platform ever supports another object store, this file is what changes — not your app code.
For the same reason STORAGE_EMULATOR_HOST is read in exactly one place (`_emulator()`):
local and deployed diverge inside this file and nowhere else.
"""
import datetime as dt
import os
import re
import time
from functools import lru_cache

_NOT_CONFIGURED = (
    "OBJECT_STORAGE_BUCKET is not set.\n"
    "  Deployed: add `object-storage: {}` under `services:` in substrait.yaml and redeploy.\n"
    "  Locally:  docker compose up -d gcs\n"
    "            export OBJECT_STORAGE_BUCKET=local-dev\n"
    "            export STORAGE_EMULATOR_HOST=http://localhost:4443"
)

# Content types a browser may PUT through a signed upload URL. An allowlist, not a
# denylist: `text/html` and `image/svg+xml` execute when fetched, so user content must
# never be stored under them. Extend this set deliberately if your app needs more.
UPLOAD_CONTENT_TYPES = frozenset({
    "application/json",
    "application/octet-stream",
    "application/pdf",
    "application/zip",
    "audio/mpeg",
    "image/gif",
    "image/jpeg",
    "image/png",
    "image/webp",
    "text/csv",
    "text/plain",
    "video/mp4",
})


class StorageNotConfigured(RuntimeError):
    """Raised on first use when the service isn't declared (deployed) or the emulator isn't
    running (locally). The message tells you which one to fix."""


def _bucket_name() -> str:                       # read per call, NOT at import
    name = os.getenv("OBJECT_STORAGE_BUCKET", "")
    if not name:
        raise StorageNotConfigured(_NOT_CONFIGURED)
    return name


def _emulator() -> str:
    return os.getenv("STORAGE_EMULATOR_HOST", "")


@lru_cache(maxsize=1)
def _client():                                   # cache the CLIENT, never the bucket
    from google.cloud import storage  # lazy: no dependency unless you use it

    if _emulator():
        from google.auth.credentials import AnonymousCredentials

        return storage.Client(credentials=AnonymousCredentials(), project="local-dev")
    return storage.Client()                      # Workload Identity — ADC, no key


def _bucket():
    # bucket() NOT get_bucket(): get_bucket() needs storage.buckets.get, which the app's
    # identity deliberately does not have.
    name = _bucket_name()                        # first: an unconfigured app must get the
    c = _client()                                # explanatory error, not an SDK/ADC failure
    b = c.bucket(name)
    if _emulator() and not b.exists():
        b = c.create_bucket(name)                # emulator convenience only
    return b


def _signer():
    """ADC, refreshed, as a *real* service account.

    Workload Identity gives the pod its own Google service account, so V4 signing can go
    through the IAM signBlob API: no private key is mounted, downloaded or needed.
    """
    import google.auth
    from google.auth.transport.requests import Request

    creds, _ = google.auth.default()
    creds.refresh(Request())
    return creds


# Deliberately narrow: ASCII letters, digits, dot, underscore, dash and the `/` separator.
# Anything else — a space, an accent, `+`, `%`, `#`, `?` — is REJECTED rather than escaped,
# because a key you have to escape is a key you will one day forget to escape. Derive keys
# yourself and keep the user's filename in a database column; then this never fires.
#
# Matched with `fullmatch`, and the anchors are gone on purpose. Python's `$` matches at the
# end of the string OR immediately before a trailing newline, so `re.match(r"^…$", key)` let a
# key ending in a LINE FEED through this otherwise airtight charset — one of exactly two
# characters GCS refuses outright. `\r` was caught and `\n` was not.
_SAFE_KEY = re.compile(r"[A-Za-z0-9._/-]{1,512}")

# GCS reserves this prefix for ACME HTTP-01 challenge tokens and refuses to store any object
# under it. fake-gcs-server has no such rule, so without this check the refusal first appears
# in production.
_RESERVED_PREFIX = ".well-known/acme-challenge/"


def safe_key(*parts: str) -> str:
    """Build an object key from untrusted input, or raise ValueError.

    NEVER use a client-supplied filename as a key directly: a `/` crosses prefixes and `..`
    is not neutralised by GCS. Note the charset above is stricter than filenames are — call
    this on your OWN derived key and catch ValueError at any boundary where a client can
    still influence it (a 400, never a 500).

    An empty part RAISES rather than being skipped, and that is the important one: with
    `safe_key(tenant_id, name)`, a request that sent no tenant at all — `?tenant_id=` is a
    perfectly valid empty string — would otherwise drop the prefix and land the object in the
    bucket root, outside every tenant's namespace and shared with every other caller who
    also arrived without one. A prefix that can silently vanish is not a boundary.

    Every name GCS itself REFUSES is refused here, because the local emulator enforces none of
    them and a key it accepts must not be one that 400s once you deploy. Google's four rules,
    and where each is met: a name is at most 1024 bytes (the charset is ASCII and capped at
    512 characters, so comfortably inside), contains no carriage return or line feed (the
    charset, matched with `fullmatch`), is never exactly `.` or `..` (the segment check below),
    and never starts with `.well-known/acme-challenge/` (`_RESERVED_PREFIX`).
    """
    if not parts:
        raise ValueError("an object key needs at least one part")
    for p in parts:
        if not p.strip():
            raise ValueError(
                "empty key part: a blank prefix does not put the object inside a namespace, "
                "it puts it outside every one of them"
            )
        if p.startswith("/"):
            raise ValueError("object keys are relative — an absolute path is not a key")
    key = "/".join(p.strip("/") for p in parts)
    # `.` and `..` are rejected as whole SEGMENTS rather than as substrings anywhere in the
    # key. Substring matching is wrong in both directions: it refuses the ordinary
    # `report..v2.pdf` while accepting `a/./b`. An empty segment (`a//b`) goes the same way:
    # it is the vanishing prefix again, one level down.
    #
    # Only half of this is GCS's own rule, and the halves are worth keeping straight. GCS
    # refuses an object named EXACTLY `.` or `..` ("Invalid argument"), and fake-gcs-server
    # stores it — that one is a key which would fail only in production. A `.` or `..` segment
    # *inside* a longer name (`a/./b`, `x/../y`) is a legal GCS object name that Google merely
    # recommends avoiding; refusing it is ours, defence in depth, because a key whose meaning
    # depends on whether something along the way resolved it as a path is not worth the
    # ambiguity.
    if not _SAFE_KEY.fullmatch(key) or any(s in ("", ".", "..") for s in key.split("/")):
        raise ValueError(f"unsafe object key: {key!r}")
    if key.startswith(_RESERVED_PREFIX):
        raise ValueError(
            f"reserved object key prefix: {key!r} — GCS refuses to store anything under "
            f"{_RESERVED_PREFIX!r}, however ordinary the rest of the key looks"
        )
    return key


_JSON_ERROR_CODE = re.compile(r'"code"\s*:\s*(\d{3})')


def _is_403(exc: Exception) -> bool:
    """Best effort: does this transport failure carry an HTTP 403?

    The IAM signBlob API's error arrives as a google.auth TransportError whose text embeds
    the API's JSON body, so read `error.code` out of it when it is there — that is the case
    that matters, and it is exact. Only when there is no parseable body do we fall back to
    searching the text, and that fallback is a heuristic rather than a test: it still says
    yes to a connection error naming `svc-403@…`, or to a read timeout of `60.403`.

    Deliberately left permissive. A false positive costs at most a few needless retries and
    changes no outcome — the exception is re-raised after the last attempt either way — while
    a false negative would mean never retrying a genuine first-deploy propagation 403, which
    is the whole reason this function exists.
    """
    text = str(exc)
    m = _JSON_ERROR_CODE.search(text)
    if m:
        return m.group(1) == "403"
    return "PERMISSION_DENIED" in text or re.search(r"(?<!\d)403(?!\d)", text) is not None


def _retry_403(fn, *, attempts: int = 4, backoff: float = 1.0):
    """Right after a first deploy the bucket grant — and the permission to SIGN — can take a
    few seconds to propagate, so the app's very first call may see a 403. Bounded retry, then
    raise for real. Every public function below goes through this.

    Two different exceptions carry that 403. Object reads and writes raise api_core's
    Forbidden. V4 signing goes through the IAM signBlob API, which raises a google.auth
    TransportError carrying the response body instead — hence `_is_403`, which is what keeps
    an unrelated transport failure from being retried as if it were propagation.

    Signing passes a shorter `backoff` than object IO does. Both 403s have the same cause in
    that first minute, but afterwards a signing 403 is a configuration fault that will not
    fix itself, and every attempt costs the caller a real wait — see `download_url`.
    """
    from google.api_core.exceptions import Forbidden
    from google.auth.exceptions import TransportError

    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    for i in range(attempts):
        try:
            return fn()
        except Forbidden:
            if i == attempts - 1:
                raise
        except TransportError as exc:
            if i == attempts - 1 or not _is_403(exc):
                raise
        time.sleep(backoff * 2 ** i)


def put_bytes(key: str, data: bytes, *, content_type: str = "application/octet-stream") -> str:
    """Store `data` under `key` and return the key.

    The content type is whatever you pass — never echo back a client-supplied one unchecked.
    Look the client's value up in your own allowlist and store the value YOU matched, not the
    header (it may carry a `;charset=` or differ in case). See UPLOAD_CONTENT_TYPES for the
    types this module considers safe to serve back.
    """
    blob = _bucket().blob(key)
    _retry_403(lambda: blob.upload_from_string(data, content_type=content_type))
    return key


def get_bytes(key: str) -> bytes:
    """Read the whole object. For anything large, hand the caller a `download_url()`
    instead of streaming the bytes through your app."""
    blob = _bucket().blob(key)
    return _retry_403(lambda: blob.download_as_bytes())


def exists(key: str) -> bool:
    blob = _bucket().blob(key)
    return bool(_retry_403(lambda: blob.exists()))


def delete(key: str) -> None:
    """No-op when the object is already gone."""
    blob = _bucket().blob(key)                   # before the SDK import, so an unconfigured
    from google.api_core.exceptions import NotFound  # app still gets the explanatory error

    try:
        _retry_403(lambda: blob.delete())
    except NotFound:
        pass


def list_keys(prefix: str = "") -> list[str]:
    b = _bucket()
    return _retry_403(lambda: [x.name for x in b.client.list_blobs(b, prefix=prefix)])


def download_url(key: str, *, expires_seconds: int = 900, filename: str | None = None) -> str:
    """A time-limited URL anyone can GET — hand it to a browser instead of streaming bytes
    through your app. Signed with the app's own Google identity via the IAM signBlob API
    (there is no private key in the pod, and you don't need one).

    Always served as an attachment: an uploaded .html or .svg rendered inline would execute
    on storage.googleapis.com.

    Locally, fake-gcs-server does not implement V4 signing, so this returns the emulator's
    plain object URL — fine to develop against; expiry is verified once you deploy.

    Blocking, and on a persistent signing 403 it stays blocking for a second or so while it
    retries. From an `async def` handler, hop out of the event loop — see the module
    docstring.
    """
    if _emulator():
        # No signature, no expiry, and no Content-Disposition — the emulator's plain object
        # URL, so `filename` and `expires_seconds` do nothing locally. See the docs.
        return f"{_emulator()}/{_bucket_name()}/{key}"
    name = filename or key.rsplit("/", 1)[-1]
    blob = _bucket().blob(key)                   # the bucket name first, so an unconfigured
    creds = _signer()                            # app gets _NOT_CONFIGURED, not an ADC error
    return _retry_403(lambda: blob.generate_signed_url(
        version="v4",
        expiration=dt.timedelta(seconds=expires_seconds),
        method="GET",
        response_disposition=f'attachment; filename="{name}"',
        service_account_email=creds.service_account_email,
        access_token=creds.token,
    ), attempts=3, backoff=0.5)


def upload_url(
    key: str,
    *,
    expires_seconds: int = 300,
    content_type: str = "application/octet-stream",
    max_bytes: int = 32 * 1024 * 1024,
) -> str:
    """A time-limited URL a browser can PUT straight to, so large uploads never pass through
    your app. The client must send the same Content-Type AND respect the size range, or GCS
    rejects the upload — that size cap is your only defence against an unbounded write.

    NOT AVAILABLE LOCALLY: fake-gcs-server serves downloads at the object path but takes
    uploads on a different endpoint, so a signed PUT URL cannot be emulated. Develop the
    upload path against a deployed environment, or use put_bytes() locally.

    Blocking, like `download_url` — see the module docstring before calling it from
    `async def`.
    """
    if _emulator():
        raise NotImplementedError(
            "upload_url() is not supported by the local emulator — use put_bytes() locally, "
            "or test browser-direct uploads against a deployed environment."
        )
    if content_type not in UPLOAD_CONTENT_TYPES:
        raise ValueError(
            f"content type {content_type!r} is not in UPLOAD_CONTENT_TYPES. Allowlist the "
            "types your app accepts; never sign one a client chose, and never store user "
            "content as text/html or image/svg+xml."
        )
    blob = _bucket().blob(key)                   # bucket name first — see download_url()
    creds = _signer()
    return _retry_403(lambda: blob.generate_signed_url(
        version="v4",
        expiration=dt.timedelta(seconds=expires_seconds),
        method="PUT",
        content_type=content_type,
        headers={"x-goog-content-length-range": f"0,{max_bytes}"},
        service_account_email=creds.service_account_email,
        access_token=creds.token,
    ), attempts=3, backoff=0.5)
