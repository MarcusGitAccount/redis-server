import socket
import threading

def parse_resp(data):
    lines = data.split('\r\n')
    response_type = lines[0][0]
    content = lines[0][1:]

    if response_type == '+':  # Simple String
        return content
    elif response_type == '-':  # Error
        return Exception(content)
    elif response_type == ':':  # Integer
        return int(content)
    elif response_type == '$':  # Bulk String
        length = int(content)
        if length == -1:
            return None  # Null Bulk String
        return lines[1]
    elif response_type == '*':  # Array
        count = int(content)
        elements = []
        for i in range(1, 2 * count, 2):
            element = lines[i][0] + lines[i + 1]
            elements.append(parse_resp(element))
        return elements
    else:
        raise ValueError("Unknown RESP data type")


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
            tokens: list[str] = data.split('\r\n')
            print(f"Received from {client_address}: {tokens}: {parse_resp(data)}")

            if "ping" in data.lower():
                client_socket.send("+PONG\r\n".encode())

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
            client_socket, client_address = server_socket.accept() # wait for client
            client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address))
            client_thread.start()
            threads.append(client_thread)
    finally:
        server_socket.close()
        for thread in threads:
            thread.join()  # Wait for all client threads to finish
        print("Server has been gracefully shutdown.")


if __name__ == "__main__":
    main()
