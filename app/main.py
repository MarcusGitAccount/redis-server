import socket

def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client_socket, _ = server_socket.accept()  # wait for client

    while True:
        request: bytes = client_socket.recv(512)
        # For this stage, we'll just return +PONG\r\n for any input received.
        client_socket.send(b"+PONG\r\n")
   
    client_socket.close()
    server_socket.close()


if __name__ == "__main__":
    main()
