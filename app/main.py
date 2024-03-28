import socket
import threading
import time
import argparse
import random
import string

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
        return f"${len(data)}\r\n{data.decode('utf-8')}\r\n"
    elif isinstance(data, list):  # Array
        encoded_elements = "".join([encode_resp(element) for element in data])
        return f"*{len(data)}\r\n{encoded_elements}"
    else:
        raise TypeError(f"Unsupported data type: {type(data)}")


def unix_timestamp() -> int:
    return int(time.time_ns() // 1_000_000)  # miliseconds


def serialize_dataclass(instance) -> list[str]:
    data_dict = asdict(instance)
    return [f"{key}:{value}" for key, value in data_dict.items()]


def random_str(n: int = 40) -> str:
    characters = string.ascii_letters + string.digits
    random_str = "".join(random.choice(characters) for _ in range(n))
    return random_str


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


def handle_client(
    client_socket: socket, client_address: str, replication_info: ReplicationInfo
) -> None:
    print(f"New connection: {client_address}")

    while True:
        try:
            # Receive data from client
            request = client_socket.recv(128)
            timestamp: int = unix_timestamp()  # client sent data, get timestamp

            if not request:
                # No more data from client, close connection
                print(f"Connection closed by client: {client_address}")
                break

            data: str = request.decode()
            data_decoded, _ = decode_resp(data)
            command: str = data_decoded[0].lower()
            print(f"Received from {client_address}: {data_decoded} at {timestamp}")

            multipart_response: list[str] = [encode_resp(None)]
            if "ping" == command:
                multipart_response = [encode_resp("PONG")]
            elif "echo" == command:
                multipart_response = [encode_resp(data_decoded[1])]
            elif "set" == command:
                key: str = data_decoded[1]
                value: object = data_decoded[2]
                expiry_timestamp: int = MAX_32BIT_TIMESTAMP

                if len(data_decoded) == 5 and "px" == data_decoded[3]:
                    expiry_timestamp = int(data_decoded[4]) + timestamp

                with database_lock:
                    database[key] = ValueItem(value, expiry_timestamp)

                multipart_response = [encode_resp("OK")]
            elif "get" == command:
                key: str = data_decoded[1]
                with database_lock:
                    value_item: ValueItem = database.get(key, not_found)
                    print(value_item)
                    if (
                        value_item.expiry_timestamp is not None
                        and timestamp >= value_item.expiry_timestamp
                    ):
                        value_item = not_found
                        del database[key]

                multipart_response = [encode_resp(value_item.value)]
            elif "info" == command:
                multipart_response = [
                    encode_resp("\n".join(serialize_dataclass(replication_info)))
                ]
            elif "replconf" == command:
                multipart_response = [encode_resp("OK")]

            for part in multipart_response:
                client_socket.send(part.encode("utf-8"))

        except Exception as e:
            print(f"Error with {client_address}: {e}")
            break

    client_socket.close()


def send_messages_to_master(messages: list[str], server_info: tuple[str, int]) -> str:
    """
    Assumess message is RESP encoded
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:  # tcp over ipv4
        sock.connect(server_info)
        for message in messages:
            pprint(f"Sending message to master: {message}")
            sock.sendall(message.encode("utf-8"))
            response: bytes = sock.recv(128)
            pprint(f"Master responded with {response.decode(errors='ignore')}")
    return response


def run_slave_configuration(
    master_host: str,
    master_port: int,
    own_port: int,
) -> None:
    master_info: tuple[str, int] = (master_host, master_port)

    print("Server is in [SLAVE] mode, will try to connect to master...")
    print(f"Pinging master over: {master_info}")
    send_messages_to_master([
        encode_resp(["PING"]),
        encode_resp(["REPLCONF", "listening-port", str(own_port)]),
        encode_resp(["REPLCONF", "capa", "psync2"]),
        encode_resp(["PSYNC", "?", "-1"]),
    ], master_info,)


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

    role: str = MASTER_REPLICATION
    if args.replicaof is not None:
        role = SLAVE_REPLICATION
        host, port_str = args.replicaof
        try:
            port = int(port_str)
        except ValueError:
            parser.error("PORT must be an integer")
        run_slave_configuration(host, port, args.port)

    replication_info = ReplicationInfo(role=role)

    server_socket = socket.create_server(("localhost", args.port), reuse_port=True)
    server_socket.listen()
    threads = []

    print(f"Server is listening for connections on port {args.port}...")

    try:
        while True:
            client_socket, client_address = server_socket.accept()  # wait for client
            client_thread = threading.Thread(
                target=handle_client,
                args=(client_socket, client_address, replication_info),
            )
            client_thread.daemon = True  # daemon threads are killed automatically when the main program exits
            client_thread.start()
            threads.append(client_thread)
    finally:
        server_socket.close()
        for thread in threads:
            thread.join()  # Wait for all client threads to finish
        print("Server has been gracefully shutdown.")


if __name__ == "__main__":
    start_redis_server()
    # print(decode_resp("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
    # print(parse_resp(":-123\r\n"))
    # resp: str ='\r\n'.join(['*2', '$4', 'echo', '$5', 'mango', ''])
    # print(resp)
    # print(parse_resp(resp))
    # print(parse_resp('*1\r\n$4\r\nping\r\n'))
    # print(encode_resp(None))

    """
                    1711644520407554
    expiry_timestamp=1711644520407654
                    1711644520410214
    """
