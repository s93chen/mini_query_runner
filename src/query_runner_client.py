import socket
import argparse
import utils as utl


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Query Runner")
    parser.add_argument("-host", required=True, help="IPv4 host IP address")
    parser.add_argument("-port", required=True, help="Server port")

    args = parser.parse_args()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((args.host, int(args.port)))

        print(f"Client: {s.getsockname()}")
        print(f"Connection established: {s.getpeername()}")

        while True:
            query = input("> Query: ")

            utl.send_msg(query, s)
            query_result, _ = utl.receive_msg(s)

            print(query_result)
