import socket


def is_socket_open(ip, port):
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        connection.connect((ip, int(port)))
        connection.shutdown(2)
        return True
    except Exception:
        return False
