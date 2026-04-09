from pylon.core.tools import log  # pylint: disable=E0611,E0401

import argparse
import os
import socket
import ssl
import struct
import threading
import time
from typing import Optional

# Constants for PostgreSQL protocol
PG_SSL_REQUEST_CODE = 80877103  # 0x04D2162F
PG_GSSENC_REQUEST_CODE = 80877104
PG_PROTOCOL_3_0 = 196608       # 3.0
AUTH_OK = 0
AUTH_CLEARTEXT_PASSWORD = 3

# Scope for Azure Database for PostgreSQL AAD tokens
AZURE_PG_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"

def read_exact(sock: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("socket closed")
        data += chunk
    return data

def read_int32(sock: socket.socket) -> int:
    b = read_exact(sock, 4)
    return struct.unpack("!I", b)[0]

def write_all(sock: socket.socket, data: bytes):
    total = 0
    while total < len(data):
        sent = sock.send(data[total:])
        if sent <= 0:
            raise ConnectionError("socket closed")
        total += sent

def read_startup_or_request(sock: socket.socket) -> bytes:
    """
    Read initial message from client:
    - Could be SSLRequest (len=8 + code)
    - Could be GSSENCRequest (len=8 + code)
    - Or a StartupMessage (len>=8 + protocol + params)
    Returns the raw bytes of the StartupMessage when encountered.
    For SSL/GSS requests, this function handles the response ('N') and loops to read the next message.
    """
    while True:
        # Peek at first 4 bytes for length
        first4 = read_exact(sock, 4)
        msg_len = struct.unpack("!I", first4)[0]
        if msg_len < 8:
            raise ValueError("Invalid startup/request length")

        payload = read_exact(sock, msg_len - 4)
        # Examine protocol/request code
        if msg_len == 8:
            code = struct.unpack("!I", payload)[0]
            if code in (PG_SSL_REQUEST_CODE, PG_GSSENC_REQUEST_CODE):
                # Decline (send 'N') to keep client in cleartext
                write_all(sock, b'N')
                # Continue to read the next message (should be Startup)
                continue
            else:
                # Unknown 8-byte request
                raise ValueError(f"Unknown initial request code: {code}")
        else:
            # This should be a StartupMessage; return full bytes (length + payload)
            # Protocol version is first 4 bytes of payload
            proto = struct.unpack("!I", payload[:4])[0]
            if proto != PG_PROTOCOL_3_0:
                # Some tools use cancel requests etc; not supported by this proxy
                # We will just pass it through as startup anyway
                pass
            return first4 + payload

def read_typed_message(sock: socket.socket) -> tuple[bytes, bytes]:
    """
    Reads a typed Frontend/Backend message:
    - 1 byte tag
    - 4 bytes length (includes length field, excludes tag)
    - length-4 bytes body
    Returns (tag, body).
    """
    tag = read_exact(sock, 1)
    length = read_int32(sock)
    if length < 4:
        raise ValueError("Invalid message length")
    body = read_exact(sock, length - 4)
    return tag, body

def send_typed_message(sock: socket.socket, tag: bytes, body: bytes):
    length = 4 + len(body)
    write_all(sock, tag + struct.pack("!I", length) + body)

def start_tls_over_postgres(sock: socket.socket, server_host: str) -> ssl.SSLSocket:
    """
    Performs Postgres SSLRequest, expects 'S', then upgrades the existing TCP socket to TLS.
    """
    # Send SSLRequest
    write_all(sock, struct.pack("!I", 8) + struct.pack("!I", PG_SSL_REQUEST_CODE))
    resp = read_exact(sock, 1)
    if resp != b'S':
        raise ConnectionError("Server did not accept SSL")
    # Wrap with TLS
    ctx = ssl.create_default_context()
    # Azure Database for PostgreSQL uses valid certs; SNI is important
    tls_sock = ctx.wrap_socket(sock, server_hostname=server_host)
    return tls_sock

def pipe(src: socket.socket, dst: socket.socket):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            write_all(dst, data)
    except Exception:
        log.exception("Proxy pipe exception")
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except Exception:
            pass

class TokenProvider:
    def __init__(self, scope: str):
        # Azure Identity for token acquisition
        from azure.identity import DefaultAzureCredential
        #
        self.cred = DefaultAzureCredential(exclude_interactive_browser_credential=False)
        self.scope = scope
        self._token = None
        self._expires = 0.0
        self._lock = threading.Lock()

    def get_token(self) -> str:
        with self._lock:
            now = time.time()
            # Refresh if less than 5 minutes remaining
            if self._token is None or now > (self._expires - 300):
                access_token = self.cred.get_token(self.scope)
                self._token = access_token.token
                # azure-identity returns expires_on as epoch
                self._expires = float(access_token.expires_on)
            return self._token

def set_keepalive(sock, *, after_idle_sec=30, interval_sec=10, max_probes=5):
    # Enable basic keepalive
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    # Linux
    if hasattr(socket, 'TCP_KEEPIDLE'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec)
    if hasattr(socket, 'TCP_KEEPINTVL'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
    if hasattr(socket, 'TCP_KEEPCNT'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_probes)

    # macOS: TCP_KEEPALIVE is the idle time (seconds)
    if hasattr(socket, 'TCP_KEEPALIVE'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, after_idle_sec)

    # Windows: optional tuning via SIO_KEEPALIVE_VALS (milliseconds)
    # Requires socket.ioctl; if not available or you don’t need tuning, skip this.
    try:
        SIO_KEEPALIVE_VALS = 0x98000004
        # enable=1, keepalive_time_ms, keepalive_interval_ms
        import struct
        sock.ioctl(SIO_KEEPALIVE_VALS, struct.pack('III', 1, after_idle_sec * 1000, interval_sec * 1000))
    except Exception:
        # Not supported on this platform or Python build; basic keepalive remains enabled.
        pass

def handle_client(client_sock: socket.socket, remote_host: str, remote_port: int, token_provider: TokenProvider):
    server_sock: Optional[socket.socket] = None
    try:
        # 1) Read client's StartupMessage (decline SSL/GSS)
        startup = read_startup_or_request(client_sock)

        # 2) Connect to server (TCP), then upgrade to TLS via Postgres SSLRequest
        raw_server_sock = socket.create_connection((remote_host, remote_port), timeout=10.0)
        raw_server_sock.settimeout(None)
        set_keepalive(raw_server_sock)
        server_sock = start_tls_over_postgres(raw_server_sock, server_host=remote_host)
        server_sock.settimeout(None)

        # 3) Forward client's StartupMessage to server
        write_all(server_sock, startup)

        # 4) Handle authentication preface: forward server's initial messages
        #    until it requests cleartext password (code 3) or auth OK (code 0),
        #    forwarding them to the client. If it requests cleartext, intercept
        #    the client's PasswordMessage and replace with AAD token.
        auth_phase_done = False
        replaced_password = False

        while not auth_phase_done:
            tag, body = read_typed_message(server_sock)

            # Forward message to client
            send_typed_message(client_sock, tag, body)

            if tag == b'R':
                # Authentication request/ok
                if len(body) < 4:
                    raise ValueError("Malformed Authentication message")
                (auth_code,) = struct.unpack("!I", body[:4])

                if auth_code == AUTH_OK:
                    # Auth complete; move to raw piping
                    auth_phase_done = True
                elif auth_code == AUTH_CLEARTEXT_PASSWORD:
                    # Server asked for cleartext password. Read client's 'p' message,
                    # replace its body with the AAD token, and send to server.
                    ctag, cbody = read_typed_message(client_sock)
                    if ctag != b'p':
                        # Unexpected message; just forward as-is (likely to fail)
                        send_typed_message(server_sock, ctag, cbody)
                    else:
                        # Replace with token + null terminator
                        token = token_provider.get_token().encode("utf-8") + b"\x00"
                        send_typed_message(server_sock, b'p', token)
                        replaced_password = True
                    # After this, continue loop and forward next server messages,
                    # but we can break to raw piping; remaining messages will flow.
                    auth_phase_done = True
                else:
                    # Other auth methods not handled specially; proceed to raw piping
                    auth_phase_done = True
            else:
                # Other server messages before auth request (rare), keep forwarding
                pass

        # 5) Start bidirectional raw piping
        t1 = threading.Thread(target=pipe, args=(server_sock, client_sock), daemon=True)
        t2 = threading.Thread(target=pipe, args=(client_sock, server_sock), daemon=True)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

    except Exception as e:
        log.exception("Proxy client exception")
        try:
            # Send an ErrorResponse to client if we can
            # ErrorResponse: 'E' + length + fields ('S' severity, 'M' message, zero terminator, overall zero terminator)
            msg = f"proxy error: {e}".encode("utf-8")
            fields = b'S' + b'FATAL' + b'\x00' + b'M' + msg + b'\x00' + b'\x00'
            payload_len = 4 + len(fields)
            pkt = b'E' + struct.pack('!I', payload_len) + fields
            write_all(client_sock, pkt)
        except Exception:
            pass
    finally:
        try:
            client_sock.close()
        except Exception:
            pass
        if server_sock:
            try:
                server_sock.close()
            except Exception:
                pass

def postgres_proxy(bind, remote, scope=AZURE_PG_SCOPE):
    bind_host, bind_port_str = bind.split(":")
    bind_port = int(bind_port_str)

    remote_host, remote_port_str = remote.split(":")
    remote_port = int(remote_port_str)

    token_provider = TokenProvider(scope=scope)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((bind_host, bind_port))
        srv.listen(100)
        log.info(f"Proxy listening on {bind_host}:{bind_port} -> {remote_host}:{remote_port}")
        while True:
            client_sock, addr = srv.accept()
            client_sock.settimeout(None)
            set_keepalive(client_sock)
            t = threading.Thread(target=handle_client, args=(client_sock, remote_host, remote_port, token_provider), daemon=True)
            t.start()

def start_postgres_proxy(bind, remote, scope=AZURE_PG_SCOPE):
    t = threading.Thread(target=postgres_proxy, args=(bind, remote, scope), daemon=True)
    t.start()
