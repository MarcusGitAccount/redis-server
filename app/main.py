import socket
import threading
import time
import argparse
import random
import string

from collections import deque
from pprint import pprint
from dataclasses import dataclass, field, asdict


def decode_resp(data, start_index=0) -> tuple[list, int]:
    data_type = data[start_index]
    # Determine the end of the current segment based on the data type
    end_index = data.find("\r\n", start_index)
    content = data[start_index + 1 : end_index]

    if data_type == "+":  # Simple String
        return content, end_index + 2
    elif data_type == "-":  # Error
        return Exception(content), end_index + 2
    elif data_type == ":":  # Integer
        return int(content), end_index + 2
    elif data_type == "$":  # Bulk String
        length = int(content)
        if length == -1:  # Handle Null Bulk String
            return None, end_index + 2
        start_of_content = end_index + 2
        end_of_content = start_of_content + length
        return data[start_of_content:end_of_content], end_of_content + 2
    elif data_type == "*":  # Array
        count = int(content)
        elements = []
        current_index = end_index + 2
        for _ in range(count):
            element, next_index = decode_resp(data, current_index)
            elements.append(element)
            current_index = next_index
        return elements, current_index
    else:
        raise ValueError(f"Unknown RESP data type: {data_type}")


def encode_resp(data: object) -> str:
    if isinstance(data, str):  # Bulk String
        return f"${len(data)}\r\n{data}\r\n"
    elif isinstance(data, Exception):  # Error
        return f"-{str(data)}\r\n"
    elif isinstance(data, int):  # Integer
        return f":{data}\r\n"
    elif data is None:  # Null Bulk String
        return "$-1\r\n"
    elif isinstance(data, bytes):  # Bulk String from bytes
        return f"${len(data)}\r\n{data.decode('utf-8', errors='ignore')}"
    elif isinstance(data, list):  # Array
        encoded_elements = "".join([encode_resp(element) for element in data])
        return f"*{len(data)}\r\n{encoded_elements}"
    else:
        raise TypeError(f"Unsupported data type: {type(data)}")


def decode_multiple_resp_commands(data: str) -> list[list[str]]:
    index = 0
    result = []
    while index < len(data):
        try:
            curr, next_index = decode_resp(data, start_index=index)
        except:
            break
        index = next_index
        result.append(curr)
    return result


def unix_timestamp() -> int:
    return int(time.time_ns() // 1_000_000)  # miliseconds


def serialize_dataclass(instance) -> list[str]:
    data_dict = asdict(instance)
    return [
        f"{key}:{value}" for key, value in data_dict.items() if not key.startswith("__")
    ]


def random_str(n: int = 40) -> str:
    characters = string.ascii_letters + string.digits
    random_str = "".join(random.choice(characters) for _ in range(n))
    return random_str


EMPTY_RDB: str = (
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)
MAX_32BIT_TIMESTAMP = (2**31 - 1) * 1_000
MASTER_REPLICATION: str = "master"
SLAVE_REPLICATION: str = "slave"


@dataclass
class ReplicationInfo:
    role: str = MASTER_REPLICATION
    # connected_slaves: int
    master_replid: str = field(default_factory=random_str)
    master_repl_offset: int = 0
    # second_repl_offset: int
    # repl_backlog_active: int
    # repl_backlog_size: int
    # repl_backlog_first_byte_offset: int
    # repl_backlog_histlen: int = 0


@dataclass
class ValueItem:
    value: object
    expiry_timestamp: int = field(init=True, default=MAX_32BIT_TIMESTAMP)


not_found = ValueItem(None, None)
database_lock = threading.Lock()
database: dict[str, ValueItem] = dict()
replica_sockets: list[socket.socket] = []


def handle_ping():
    return encode_resp("PONG")


def handle_echo(data_decoded):
    return encode_resp(data_decoded[1])


def handle_set(
    data_decoded,
    client_socket,
    client_address,
    replica_sockets,
    database_lock,
    database,
    replication_info,
    timestamp,
    data,
):
    with database_lock:
        for replica_socket in replica_sockets:
            replicate([data], replica_socket)

        key = data_decoded[1]
        value = data_decoded[2]
        expiry_timestamp = MAX_32BIT_TIMESTAMP

        if len(data_decoded) == 5 and "px" == data_decoded[3]:
            expiry_timestamp = int(data_decoded[4]) + timestamp

        database[key] = ValueItem(value, expiry_timestamp)

        return encode_resp("OK")


def handle_get(data_decoded, database_lock, database, timestamp):
    with database_lock:
        key = data_decoded[1]
        value_item = database.get(key, not_found)
        if (
            value_item.expiry_timestamp is not None
            and timestamp >= value_item.expiry_timestamp
        ):
            value_item = not_found
            del database[key]

        return encode_resp(value_item.value)


def handle_info(replication_info):
    return encode_resp("\n".join(serialize_dataclass(replication_info)))


def handle_replconf():
    return encode_resp("OK")


def handle_psync(data_decoded, client_address, client_socket, replica_sockets):
    replication_id = data_decoded[1]
    if replication_id == "?":
        new_replication_id = random_str(n=40)
        response = f"FULLRESYNC {new_replication_id} 0"
        rdb_bytes = bytes.fromhex(EMPTY_RDB)
        replica_sockets.append(client_socket)
        return [
            encode_resp(response),
            f"${len(rdb_bytes)}\r\n".encode("utf-8") + rdb_bytes,
        ]

    return None


def handle_client(
    client_socket: socket.socket,
    client_address: str,
    replication_info: ReplicationInfo,
    send_response_back: bool = True,
    buffer_size: int = 1024,
):
    print(f"New connection: {client_address}")
    keep_client_socket_open = False

    while True:
        try:
            request = client_socket.recv(buffer_size)
            if not request:
                print(f"Connection closed by client: {client_address}")
                break

            timestamp = unix_timestamp()
            data = request.decode()
            data_decoded, _ = decode_resp(data)
            command = data_decoded[0].lower()
            print(f"Received from {client_address}: {data_decoded} at {timestamp}")
            log_data: str = data.replace("\r\n", "\\r\\n")
            print(f"Raw data: {log_data}")

            if command == "ping":
                response = handle_ping()
            elif command == "echo":
                response = handle_echo(data_decoded)
            elif command == "set":
                response = handle_set(
                    data_decoded,
                    client_socket,
                    client_address,
                    replica_sockets,
                    database_lock,
                    database,
                    replication_info,
                    timestamp,
                    data,
                )
            elif command == "get":
                response = handle_get(data_decoded, database_lock, database, timestamp)
            elif command == "info":
                response = handle_info(replication_info)
            elif command == "replconf":
                response = handle_replconf()
            elif command == "psync":
                response = handle_psync(
                    data_decoded, client_address, client_socket, replica_sockets
                )
                keep_client_socket_open = True

            if not send_response_back:
                continue
            if response:
                if isinstance(response, list):
                    for part in response:
                        client_socket.send(
                            part if isinstance(part, bytes) else part.encode("utf-8")
                        )
                else:
                    client_socket.send(response.encode("utf-8"))

        except Exception as e:
            print(f"Error with {client_address}: {e}")
            break

    if not keep_client_socket_open:
        client_socket.close()


def handle_master_conn(
    master_socket: socket.socket,
) -> None:
    """
    Assumess message is RESP encoded. 1-2 messages are expected after
    """
    # Continue listening for messages from master
    while True:
        try:
            incoming_message = master_socket.recv(1024)

            if not incoming_message:
                print("Connection closed by master...")
                break

            timestamp = unix_timestamp()
            data = incoming_message.decode(errors="ignore")

            log_data: str = data.replace("\r\n", "\\r\\n")
            print(f"Raw data: {log_data}")
            commands = decode_multiple_resp_commands(data)
            pprint(f"Received from master replication commands: {commands}")

            for command in commands:
                if command[0].lower() == "set":
                    handle_set(
                        command,
                        None,
                        None,
                        replica_sockets,
                        database_lock,
                        database,
                        None,
                        timestamp,
                        data
                    )
                elif command[0].lower() == "replconf":
                    response: list = encode_resp(["REPLCONF", "ACK", str(0)])
                    master_socket.send(response.encode("utf-8"))
        except Exception as e:
            print(f"Error with master connection...")
            break

    master_socket.close()


def replicate(messages: list[str], sock: socket.socket) -> None:
    """
    Assumess message is RESP encoded
    """
    for message in messages:
        pprint(f"Replicating {message} to {sock}")
        sock.sendall(message.encode("utf-8"))


def start_redis_server():
    parser = argparse.ArgumentParser(description="Example script.")
    parser.add_argument(
        "--port", help="Redis server port, defaults to 6379", default=6379, type=int
    )
    parser.add_argument(
        "--replicaof",
        help="Master address & port",
        metavar=("HOST", "PORT"),
        nargs=2,
        default=None,
        type=str,
    )
    args = parser.parse_args()
    master_info: tuple[str, int] = None
    master_thread: threading.Thread = None

    role: str = MASTER_REPLICATION
    if args.replicaof is not None:
        role = SLAVE_REPLICATION
        host, port_str = args.replicaof
        try:
            port = int(port_str)
        except ValueError:
            parser.error("PORT must be an integer")

        print("Server is in [SLAVE] mode, will try to connect to master...")
        master_socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM
        )  # tcp over ipv4
        master_info = (host, port)
        master_socket.connect(master_info)

        print(f"Pinging master over: {master_info}")

        master_socket.sendall(encode_resp(["PING"]).encode())
        assert master_socket.recv(128).decode().strip() == "+PONG"

        master_socket.sendall(
            encode_resp(["REPLCONF", "listening-port", str(args.port)]).encode()
        )
        assert master_socket.recv(1024).decode().strip() == "+OK"

        master_socket.sendall(encode_resp(["REPLCONF", "capa", "psync2"]).encode())
        assert master_socket.recv(1024).decode().strip() == "+OK"

        master_socket.sendall(encode_resp(["PSYNC", "?", "-1"]).encode())
        psync_msg = master_socket.recv(56).decode()
        rdb_msg = master_socket.recv(2048)

        master_thread = threading.Thread(
            target=handle_master_conn,
            args=(master_socket,),
        ).start()

        # print("Waiting a bit to accept new client connections...")
        # time.sleep(1.5)

    server_socket = socket.create_server(("localhost", args.port), reuse_port=True)
    server_socket.listen()
    threads = []

    print(f"Server is listening for connections on port {args.port}...")
    try:
        while True:
            client_socket, client_address = server_socket.accept()  # wait for client
            client_thread = threading.Thread(
                target=handle_client,
                args=(
                    client_socket,
                    client_address,
                    ReplicationInfo(role=role),
                    True,
                    1024,
                ),
            )
            client_thread.daemon = True  # daemon threads are killed automatically when the main program exits
            client_thread.start()
            threads.append(client_thread)
    finally:
        server_socket.close()
        for thread in threads:
            thread.join()  # Wait for all client threads to finish
        if master_thread is not None:
            master_thread.join()
        print("Server has been gracefully shutdown.")


if __name__ == "__main__":
    start_redis_server()
    # data = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n"
    # print(decode_resp(data=data, start_index=62))
    # print(decode_multiple_resp_commands(data))
