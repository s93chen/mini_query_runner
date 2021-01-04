import socket
import config as cfg
from typing import Tuple


def send_msg(msg_body: str, sock: socket.socket) -> None:
    """
    Message consists of
    header (4 bytes indicating body size),
    and size (msg_body in bytes).
    """
    msg = make_msg(msg_body)
    sock.sendall(msg)


def receive_msg(sock: socket.socket) -> Tuple[str, int]:
    """
    Receives message from peer. If header cannot
    be correctly read (4 bytes expected), return
    error code 1.

    Return:
        message body: if invalid response, return error message
        status code: if success return 0, else 1
    """
    resp_header = receive_n_bytes(sock, cfg.HEADER_SIZE)

    if len(resp_header) == cfg.HEADER_SIZE:
        n_bytes = int.from_bytes(resp_header, cfg.BYTE_ORDER)
        output = receive_n_bytes(sock, n_bytes).decode(cfg.ENCODING)
        return output, 0

    else:
        error = "[ERROR] Failed to read message."
        return f"\033[91m{error}\033[0m", 1


def make_msg(
    body: str, 
    header_size: int = cfg.HEADER_SIZE,
    byte_order: str = cfg.BYTE_ORDER,
    encoding: str = cfg.ENCODING
) -> bytes:
    """
    Make message in format of header + body. The
    header takes the first 4 bytes of the message,
    specifying the size of body as an integer in
    big-endian representation.

    Param:
        body: the message body
    Returns:
        full message (header + body)
    """

    header = len(body).to_bytes(header_size, byte_order)
    body = body.encode(encoding)
    return header + body


def receive_n_bytes(conn: socket.socket, n_bytes: int) -> bytes:
    """
    Read n bytes, byte by byte.
    Stop if n_bytes have been read, or
    if a byte is not received.

    """

    data = b""

    while n_bytes > 0:
        res_data = conn.recv(n_bytes)
        if not res_data:
            break

        data += res_data
        n_bytes -= len(res_data)

    return data
