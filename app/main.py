import socket
import threading


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

            data = request.decode()
            print(f"Received from {client_address}: {data}")

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
    print("Server is listening for connections...")

    try:
        while True:
            client_socket, client_address = server_socket.accept() # wait for client
            client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address))
            client_thread.start()
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()
