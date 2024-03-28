import socket
import threading


def parse_resp(data, start_index=0) -> tuple[list, int]:
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
            element, next_index = parse_resp(data, current_index)
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


lock = threading.Lock()
database: dict[str, object] = dict()


def handle_client(client_socket, client_address):
    print(f"New connection: {client_address}")

    while True:
        try:
            # Receive data from client
            request = client_socket.recv(128)
            if not request:
                # No more data from client, close connection
                print(f"Connection closed by client: {client_address}")
                break

            data: str = request.decode()
            data_decoded, _ = parse_resp(data)
            command: str = data_decoded[0].lower()
            print(f"Received from {client_address}: {data_decoded}")

            response: str = encode_resp(None)
            if "ping" == command:
                response = encode_resp("PONG")
            elif "echo" == command:
                response = encode_resp(data_decoded[1])
            elif "set" == command:
                key: str = data_decoded[1]
                value: object = data_decoded[2]
                with lock:
                    database[key] = value
                response = encode_resp('OK')
            elif "get" == command:
                key: str = data_decoded[1]
                with lock:
                    value: object = database.get(key, None)
                response = encode_resp(value)

            client_socket.send(response.encode())

        except Exception as e:
            print(f"Error with {client_address}: {e}")
            break

    client_socket.close()


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen()
    threads = []

    print("Server is listening for connections...")

    try:
        while True:
            client_socket, client_address = server_socket.accept()  # wait for client
            client_thread = threading.Thread(
                target=handle_client, args=(client_socket, client_address)
            )
            client_thread.start()
            threads.append(client_thread)
    finally:
        server_socket.close()
        for thread in threads:
            thread.join()  # Wait for all client threads to finish
        print("Server has been gracefully shutdown.")


if __name__ == "__main__":
    main()
    # resp: str ='\r\n'.join(['*2', '$4', 'echo', '$5', 'mango', ''])
    # print(resp)
    # print(parse_resp(resp))
    # print(parse_resp('*1\r\n$4\r\nping\r\n'))
    # print(encode_resp(None))
