import socket

def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client_connection, client_address = server_socket.accept()  # accept client connection
    client_connection.sendall(b"+PONG\r\n")  # Send PONG response
    client_connection.close()  # Close the client connection


if __name__ == "__main__":
    main()
