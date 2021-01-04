import socket
import argparse
import utils as utl

from query_runner import QueryRunner


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Query Runner")
    parser.add_argument("-port", required=True, help="Server port")

    args = parser.parse_args()
    query_engine = QueryRunner()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", int(args.port)))
        print(f"Query Server: {s}")

        s.listen()

        while True:
            conn, addr = s.accept()
            print(f"Connection established: {conn.getpeername()}")

            with conn:

                while True:
                    msg, error = utl.receive_msg(conn)

                    if not error:
                        print(msg)

                        output = query_engine.run_query(msg)
                        output = query_engine.rows_to_string(output)
                        utl.send_msg(output, conn)

                    else:
                        utl.send_msg(msg, conn)
