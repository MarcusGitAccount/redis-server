import socket
import threading
import time
import argparse

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
    if isinstance(data, str):  # Simple String
        return f"+{data}\r\n"
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
        # Encode a Python string as a Bulk String
        return f"${len(data)}\r\n{data}\r\n"


def unix_timestamp() -> int:
    return int(time.time_ns() // 1_000_000)  # miliseconds


def serialize_dataclass(instance) -> list[str]:
    data_dict = asdict(instance)
    return [f"{key}:{value}" for key, value in data_dict.items()]


MAX_32BIT_TIMESTAMP = (2**31 - 1) * 1_000
MASTER_REPLICATION: str = "master"
SLAVE_REPLICATION: str = "slave"


@dataclass
class ReplicationInfo:
    role: str = MASTER_REPLICATION
    # connected_slaves: int
    # master_replid: str
    # master_repl_offset: int
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
                    encode_resp(key_val)
                    for key_val in serialize_dataclass(replication_info)
                ]

            for part in multipart_response:
                client_socket.send(part.encode("utf-8"))

        except Exception as e:
            print(f"Error with {client_address}: {e}")
            break

    client_socket.close()


def main():
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
        type=type[str],
    )
    args = parser.parse_args()

    role: str = MASTER_REPLICATION
    if args.replicaof is not None:
        role = SLAVE_REPLICATION

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
    main()
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
