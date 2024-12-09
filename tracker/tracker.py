import logging
import socket
import threading
import json
import psycopg2
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
stop_event = threading.Event()
# Establish a connection to the PostgreSQL database
connDB = psycopg2.connect(dbname="netApp", user="postgres", password="BachKhoa2023", host="localhost", port="5432")
print(connDB)
cur = connDB.cursor()

def log_event(message):
    logging.info(message)

# Update the client's file list in the database
def update_client_info(peers_ip, peers_port, peers_hostname, file_name, file_size, piece_hash, piece_size, num_order_in_file):
    try:
        for i in range(len(num_order_in_file)):
           
            # Check if piece hash already exists
            # piece_hash_value = piece_hash[i] if isinstance(piece_hash[i], bytes) else bytes.fromhex(piece_hash[i])
            cur.execute(
                "delete FROM peers WHERE piece_hash = %s",
                (piece_hash[i],)
            )
       

        for i in range(len(num_order_in_file)):
            # Chuyển piece_hash sang dạng byte
            # piece_hash_value = piece_hash[i] if isinstance(piece_hash[i], bytes) else bytes.fromhex(piece_hash[i])
            cur.execute(
               "INSERT INTO peers (peers_ip, peers_port, peers_hostname, file_name, file_size, piece_hash, piece_size, num_order_in_file) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (peers_ip, peers_port, file_name, num_order_in_file) DO NOTHING",  # Chỉ định các cột gây xung đột
                (peers_ip, peers_port, peers_hostname, file_name, file_size, piece_hash[i], piece_size, num_order_in_file[i])
            )
        connDB.commit()  # Commit khi không có lỗi
    except Exception as e:
        connDB.rollback()  # Rollback nếu xảy ra lỗi
        logging.exception(f"Error updating client info: {e}")


active_connections = {}
host_files = {}

# Handles client commands
def client_handler(conn, addr):
    client_peers_hostname = None
    try:
        while True:
            print("count")
            data =conn.recv(4096).decode()  
            print(f"data {data}")
            if not data:
                conn.sendall("Some Error".encode("utf-8"))
                break

            command = json.loads(data)
            action = command.get("action")
            
            # Extract client information
            peers_ip = addr[0]
            peers_port = command.get('peers_port')
            
            client_peers_hostname = command.get('peers_hostname')
            peer_ip=socket.gethostbyname(client_peers_hostname)
            update_status_peer(peers_port,peer_ip)
            # Handle each action accordingly
            if action == 'introduce':
                log_event(f"Connected to {client_peers_hostname}/{peers_ip}:{peers_port}")

            elif action == 'publish':
              
                log_event(f"Updating file info in database for {client_peers_hostname}")

                piece_size=command['file_size'] if int(command['file_size']) < int(command['piece_size']) else command['piece_size']
                update_client_info(peers_ip, peers_port, client_peers_hostname,
                                   command['file_name'], command['file_size'],
                                   command['piece_hash'], piece_size,
                                   command['num_order_in_file'])
                log_event(f"Database update complete for hostname: {client_peers_hostname}/{peers_ip}:{peers_port}")
                conn.sendall("File list updated successfully.".encode())

            elif action == 'fetch':
                file_name = command.get('file_name')
                num_order_in_file = command.get('num_order_in_file', [])
                piece_hash = command.get('piece_hash', [])
                print("piece_hash on server,num_order_in_file: ",piece_hash,num_order_in_file)
                cur.execute("""
                   SELECT *
                    FROM peers,peerstatus
                    WHERE peers.peers_ip = peerstatus.peer_ip AND peers.peers_port = peerstatus.peer_port AND peerstatus.status = 'ONLINE' AND file_name=%s
                    AND num_order_in_file <> ALL (%s)
                    AND piece_hash <> ALL (%s)
                """, (file_name, num_order_in_file, piece_hash))
                results = cur.fetchall()
                print("result of fetch file: ",results)
                if results:
                    # mảng các object
                    # print("bạn vào if rồi, yên tâm, có thể sai ở chỗ peer_infor ở dưới do dữ liệu lấy từ db là bytea đó")
                    peers_info = [{'peers_ip': r[1], 'peers_port': r[2], 'peers_hostname': r[3],
                                   'file_name': r[4], 'file_size': r[5], 'piece_hash': r[6],
                                   'piece_size': r[7], 'num_order_in_file': r[8]} for r in results]
                    conn.sendall(json.dumps({'peers_info': peers_info}).encode())
                else:
                    conn.sendall(json.dumps({'error': 'File not available'}).encode())

            elif action == 'file_list':
                files = command['files']
                print(f"List of files : {files}")
            
            elif action == 'status_update':
                status = command.get('status')
                print(f"status in update_status {status}")
                cur.execute("SELECT * FROM peerStatus WHERE peer_ip = %s AND peer_port = %s", (peers_ip, peers_port))
                result = cur.fetchone()
                print("result in update: ",result,peers_ip,peers_port)
                cur.execute(
                "UPDATE peerstatus SET status = %s WHERE peer_ip = %s AND peer_port = %s",
                ("OFFLINE", peers_ip, peers_port)
                )
                connDB.commit()
                conn.sendall(json.dumps("Update successfully").encode("utf-8"))
                log_event(f"Received {status} status from {client_peers_hostname}")

    except Exception as e:
        connDB.rollback()
        conn.sendall(json.dumps({'error': 'something wrong!'}).encode('utf-8'))
        print(f"Error in handle_client: {e}")
    # finally:
    #     if client_peers_hostname:
    #         del active_connections[client_peers_hostname]
    #     conn.close()
    #     log_event(f"Connection with {addr} closed.")

# Request file list from a specific client
def request_file_list_from_client(peer_ip,peer_port):
    try:
        peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_sock.connect((peer_ip, int(peer_port)))
        request = {'action': 'request_file_list'}
        peer_sock.sendall(json.dumps(request).encode() + b'\n')
        response = json.loads(peer_sock.recv(4096).decode())
        print(response)
        peer_sock.close()
        if "file_list" in response:
            return response['file_list']
        else:
            return "Error: No file list in response"
    except Exception as e:
        print(f"Error in request_file_List_from_client: {e}")
        return "Error: Client not connected"

# Discover files by hostname
def discover_files(peer_ip,peer_port):
    files = request_file_list_from_client(peer_ip,peer_port)
    print(f"Files on {peer_ip}:{peer_port}: {files}")

# Check if host is online by pinging
def ping_host(peers_hostname):
    print(f"ip:port {peers_hostname}")
    ip,port=peers_hostname.split(":")
    cur.execute(
        "SELECT status FROM peerstatus "
        "WHERE peer_ip=%s AND peer_port=%s",
        (ip, port)
    )
    is_online=cur.fetchone()[0]
    log_event(f"Host {peers_hostname} is {is_online}.")

# Server command shell to interact with server
def server_command_shell():
    while True:
        cmd_input = input("Server command: ")
        cmd_parts = cmd_input.split()
        if cmd_parts:
            action = cmd_parts[0]
            if action == "discover" and len(cmd_parts) == 2:
                peer_ip,peer_port=cmd_parts[1].split(":")
                cur.execute("SELECT status FROM peerstatus WHERE peer_ip=%s AND peer_port=%s ",((peer_ip),(peer_port)))
                result=cur.fetchone()
                print(f"result in request_file_list_from_client {result[0]}")
                if result[0] == "ONLINE":
                    threading.Thread(target=discover_files, args=(peer_ip,peer_port)).start()
                else:
                    print(f"Device with {peer_ip}:{peer_port} is {result[0]}")
            elif action == "ping" and len(cmd_parts) == 2:
                ping_host(cmd_parts[1])
            elif action == "exit":
                break
            else:
                log_event("Unknown command or incorrect usage.")
def update_status_peer(peer_port,peer_ip):
  
    cur.execute("SELECT * FROM peerstatus WHERE peer_ip = %s AND peer_port = %s", (peer_ip, peer_port))
    result = cur.fetchone()  # Lấy một bản ghi
    print("result: ",result)
    if not result:
        cur.execute(
                    "INSERT INTO peerstatus (peer_ip, peer_port, status)"
                    "VALUES (%s, %s, %s)",
                    (peer_ip ,peer_port,"ONLINE")
                )
        connDB.commit() 
    else:
        cur.execute(
        "UPDATE peerstatus SET status = %s WHERE peer_ip = %s AND peer_port = %s",
        ("ONLINE", peer_ip ,peer_port)
        )
        connDB.commit()  # Lưu các thay đổi vào cơ sở dữ liệu
        print("Status updated successfully.")

# Start server to listen for client connections
def start_server(host='0.0.0.0', port=65432):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(f"host - {host}")
    server_socket.bind((host, port))
    server_socket.listen()
    log_event(" Server is listening for connections!")

    try:
        while True:
            #conn: socket kết nối đến client
            #addr: địa chỉ của client gồm IP và PORT
            conn, addr = server_socket.accept()
           
            threading.Thread(target=client_handler, args=(conn, addr)).start()# Tạo luồng thực thi cho mỗi kết nối với client
    except KeyboardInterrupt:
        #nếu nhận ctrl+c, server ngừng lắng nghe, vẫn chạy các luồng đang hoạt động
          log_event("Server shutdown requested.")
    finally:
        server_socket.close()
        cur.close()
        conn.close()

if __name__ == "__main__":
    SERVER_HOST = '192.168.56.1'
    SERVER_PORT = 65432

    threading.Thread(target=start_server, args=(SERVER_HOST, SERVER_PORT),daemon=True).start()
    server_command_shell()
    print("Server Stopped!")
    sys.exit(0)
    
    
