import socket
import json
import os
import threading
import shlex
import hashlib
import psycopg2
import math
stop_event = threading.Event()

conn = psycopg2.connect(dbname="netApp", user="postgres", password="BachKhoa2023", host="localhost", port="5432")
print(conn)
cur = conn.cursor()
def calculate_piece_hash(piece_data):
    sha1 = hashlib.sha1()
    sha1.update(piece_data)
    return sha1.digest()

def create_pieces_string(pieces):
    hash_pieces = []
    for piece_file_path in pieces:
            with open(piece_file_path, "rb") as piece_file:
                piece_data = piece_file.read()
                piece_hash = calculate_piece_hash(piece_data)
                hash_pieces.append(f"{piece_hash}")
    return hash_pieces

#---------------Tách file thành các piece theo chiều dài cho trước------------#
def split_file_into_pieces(file_path, piece_length):
    pieces = []
    with open(file_path, "rb") as file:
        counter = 1
        while True:
            piece_data = file.read(piece_length)
            if not piece_data:
                break
            piece_file_path = f"{file_path}_piece{counter}"
            with open(piece_file_path, "wb") as piece_file:
                piece_file.write(piece_data)
            pieces.append(piece_file_path)
            counter += 1
    return pieces


#--------------Xáp nhập các piece lại thành file ban đầu-----------#
def merge_pieces_into_file(pieces, output_file_path):
    with open(output_file_path, "wb") as output_file:
        for piece_file_path in sorted(pieces):
            with open(piece_file_path, "rb") as piece_file:
                piece_data = piece_file.read()
                output_file.write(piece_data)

def get_list_local_files(directory='.'):
    try:
        return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    except Exception as e:
        return f"Error: Unable to list files - {e}"

def check_local_files(file_name):
    return os.path.exists(file_name)


#-----------check xem các piece của file được tải chưa------------#
def check_local_piece_files(file_name):
    exist_piece_files = []
    directory = os.getcwd()# -------- trả về thư mục làm việc hiện tại---------#
    for filename in os.listdir(directory):
        #----------- kiểm tra các piece của file đó có hay không => len(filename) > len(file_name)-----------
        if filename.startswith(file_name) and len(filename) > len(file_name):
            exist_piece_files.append(filename)
    return exist_piece_files if exist_piece_files else False

def handle_publish_piece(sock, peers_port, pieces, file_name, file_size, piece_size):
    pieces_hash = create_pieces_string(pieces)# mảng của piece đã được hash
    user_input_num_piece = input(f"File {file_name} has pieces: {pieces}\nPiece hashes: {pieces_hash}.\nSelect piece numbers to publish: ")
    num_order_in_file = shlex.split(user_input_num_piece)# kết quả là mảng của các số thứ tự piece muốn nhận
    picked_pieces_hashed_arr = []
    print("You selected: " )
    for i in num_order_in_file:
        index = pieces.index(f"{file_name}_piece{i}")
        picked_pieces_hashed_arr.append(pieces_hash[index])
        print(f"Selected Number {i}: {pieces_hash[index]}")
    publish_piece_file(sock, peers_port, file_name, file_size, picked_pieces_hashed_arr, piece_size, num_order_in_file)

# đẩy file lên server
def publish_piece_file(sock, peers_port, file_name, file_size, piece_hash, piece_size, num_order_in_file):
    peers_hostname = socket.gethostname()
    # piece_hash_hex = [h.hex() for h in piece_hash] if isinstance(piece_hash, list) else piece_hash

    # print(piece_hash_hex)
    command = {

        "action": "publish",
        "peers_port": peers_port,
        "peers_hostname": peers_hostname,
        "file_name": file_name,
        "file_size": file_size,
        "piece_hash": piece_hash,
        "piece_size": piece_size,
        "num_order_in_file": num_order_in_file,

    }
    try:
        # Gửi lệnh sau khi xác nhận JSON encoding thành công
        json_command = json.dumps(command)
        print("JSON command:", json_command)  # Kiểm tra JSON command
        sock.sendall(json_command.encode('utf-8'))
        
        # Nhận phản hồi từ server
        response = sock.recv(4096).decode('utf-8')
        print("Response from server:", response)

    except Exception as e:
        print("Error in JSON serialization or socket send:", e)

   
class PieceDownloader(threading.Thread):
    def __init__(self, peers_ip, peer_port, file_name, piece_hash, num_order_in_file):
        threading.Thread.__init__(self)
        self.peers_ip = peers_ip
        self.peer_port = peer_port
        self.file_name = file_name
        self.piece_hash = piece_hash
        self.num_order_in_file = num_order_in_file
        
    def run(self):
        peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            peer_sock.connect((self.peers_ip, int(self.peer_port)))
            peer_sock.sendall(json.dumps({
                'action': 'send_file', 
                'file_name': self.file_name, 
                'piece_hash': self.piece_hash, 
                'num_order_in_file': self.num_order_in_file
            }).encode() + b'\n')

            with open(f"{self.file_name}_piece{self.num_order_in_file}", 'wb') as f:
                while True:
                    data = peer_sock.recv(4096)
                    if not data:
                        break
                    f.write(data)

            print(f"Downloaded piece {self.num_order_in_file} of {self.file_name}")
        except Exception as e:
            print(f"Error downloading piece {self.num_order_in_file}: {e}")
        finally:
            peer_sock.close()

# #-------------request "send_file": yêu cầu file từ các peer khác-------------#
# def request_file_from_peer(peers_ip, peer_port, file_name, piece_hash, num_order_in_file):
#     # chang here:
#     peer_sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
#     # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as peer_sock:
#     command={

#         'action': "send_file",
#         'file_name': file_name,
#         'piece_hash': piece_hash,
#         'num_order_in_file': num_order_in_file

#     }
#     try:
#         peer_sock.connect((peers_ip, int(peer_port)))
#         sent_data=json.dump(command).encode('utf-8') + b'\n'
#         peer_sock.sendall(sent_data)
        
#         with open(f"{file_name}_piece{num_order_in_file}", 'wb') as f:
#             while True:
#                 data = peer_sock.recv(4096)
#                 if not data:
#                     break
#                 f.write(data)

#         print(f"Downloaded piece: {file_name}_piece{num_order_in_file} from {peers_ip}:{peer_port}")
#     except Exception as e:
#         print(f"Error connecting to peer at {peers_ip}:{peer_port} - {e}")


#------------------request "fetch" đến server--------------#
def fetch_file(sock, peers_port, file_name, piece_hash, num_order_in_file):
    peers_hostname = socket.gethostname()
    command = {
        "action": "fetch",
        "peers_port": peers_port,
        "peers_hostname":peers_hostname,
        "file_name":file_name,
        "piece_hash":piece_hash,
        "num_order_in_file":num_order_in_file,
    } 
    sock.sendall(json.dumps(command).encode() + b'\n')
    response = json.loads(sock.recv(4096).decode())
    if 'peers_info' in response:
        peers_info = response['peers_info']
        host_info_str = "\n".join([f"Number: {peer_info['num_order_in_file'] } {peer_info['peers_hostname']}/{peer_info['peers_ip']}:{peer_info['peers_port']} piece_hash: {peer_info['piece_hash']  } file_size: {peer_info['file_size']  } piece_size: {peer_info['piece_size']  } num_order_in_file: {peer_info['num_order_in_file'] }" for peer_info in peers_info])
        print(f"Hosts with the file {file_name}:\n{host_info_str}")
        if len(peers_info) >= 1:
            
            # Create and start download threads
            download_threads = []
            for peer_info in peers_info:
                    downloader = PieceDownloader(
                        peer_info['peers_ip'],
                        peer_info['peers_port'],
                        peer_info['file_name'],
                        peer_info['piece_hash'],
                        peer_info['num_order_in_file']
                    )
                    download_threads.append(downloader)
                    downloader.start()
            
            # Wait for all downloads to complete
            for thread in download_threads:
                thread.join()
            
            # Check if we have all pieces and merge if complete
            if(math.ceil(int(peers_info[0]['file_size'])/int(peers_info[0]['piece_size']))==len(sorted(pieces := check_local_piece_files(file_name)))):
                merge_pieces_into_file(pieces,file_name)
        else:
            print("No hosts have the file.")
    else:
        print("No peers have the file or the response format is incorrect.")


#-----------request "update" to server----------------#
def send_status_update(sock, peers_port, status, file_name=""):
    if sock.fileno() == -1:  # Kiểm tra socket đã bị đóng
        print("Socket is closed. Reinitializing...")
        

    if not isinstance(sock, socket.socket):
        raise ValueError("Provided object is not a valid socket")
      
    try:
        command = {
        "action": "status_update",
        "peers_port": peers_port,
        "peers_hostname": socket.gethostname(),
        "file_name": file_name,
        "status": status
        }
        sock.sendall(json.dumps(command).encode('utf-8') + b'\n')
        res=sock.recv(4096).decode();
        print(f"res: {res}")
    except Exception as e:
        print(f"Error in send_status_update: {e}")


#----------Bắt đầu gửi các piece đến client gửi yêu cầu--------------#
def send_piece_to_client(conn, piece_path):
    with open(piece_path, 'rb') as f:
        while True:
            bytes_read = f.read(4096)
            if not bytes_read:
                break
            conn.sendall(bytes_read)

#--------------Xử lý gửi piece cho các client yêu cầu------------#
def handle_file_request(conn, shared_files_dir):
    try:
        data = conn.recv(4096).decode('utf-8')
        command = json.loads(data)
        print("data recieved on sned_file: ",command)
        if command['action'] == 'send_file':
            file_name = command['file_name']
            num_order_in_file = command['num_order_in_file']
            file_path = os.path.join(shared_files_dir, f"{file_name}_piece{num_order_in_file}")
            send_piece_to_client(conn, file_path)
        elif command['action']=='request_file_list':
            file_list=get_list_local_files()
            conn.sendall(json.dumps({"file_list":file_list}).encode("utf-8"))
    except Exception as e:
        print("error in handle_file_request: ",e)
    finally:
        conn.close()


#----------- Tạo socket của client để lắng nghe yêu cầu từ client ------------------#
def start_host_service(port, shared_files_dir):
    client_service_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_service_sock.bind(('0.0.0.0', port))
    client_service_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client_service_sock.listen()
    while not stop_event.is_set():
        try:
            client_service_sock.settimeout(1)
            conn, addr = client_service_sock.accept()
            thread = threading.Thread(target=handle_file_request, args=(conn, shared_files_dir),daemon=True)
            thread.start()
        except socket.timeout:
            continue
        except Exception as e:
            print(f"Host service error: {e}")
            break
    client_service_sock.close()

def connect_to_server(server_host, server_port, peers_port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server_host, server_port))
        peers_hostname = socket.gethostname()
        sock.sendall(json.dumps({'action': 'introduce', 'peers_hostname': peers_hostname, 'peers_port': peers_port}).encode('utf-8') + b'\n')
        return sock
    except Exception as e:
        print(f"Error in connect_to server: {e}")

def main(server_host, server_port, peers_port):
    host_service_thread = threading.Thread(target=start_host_service, args=(peers_port, './'))
    host_service_thread.start()
    sock = connect_to_server(server_host, server_port, peers_port)

    try:
        while True:
            user_input = input("Enter command (publish file_name/ fetch file_name/ exit): ")
            # dùng module shlex để tách các tokens trong command, kết quả trả về là mảng
            command_parts = shlex.split(user_input)
            if len(command_parts) == 2 and command_parts[0].lower() == 'publish':
                _, file_name = command_parts
                if check_local_files(file_name):
                    piece_size = 524288
                    file_size = os.path.getsize(file_name)
                    pieces = split_file_into_pieces(file_name, piece_size)
                    handle_publish_piece(sock, peers_port, pieces, file_name, file_size, piece_size)
                elif (pieces := check_local_piece_files(file_name)):
                    handle_publish_piece(sock, peers_port, pieces, file_name, os.path.getsize(file_name), 524288)
                else:
                    print(f"Local file {file_name}/piece does not exist.")
            elif len(command_parts) == 2 and command_parts[0].lower() == 'fetch':
                # file_name chứa tên của file muốn lấy từ server.
                _, file_name = command_parts
                # check local_piece để loại trừ ở phía query vào db
                pieces = check_local_piece_files(file_name)
                print("local piece on client !")
                pieces_hash = [] if not pieces else create_pieces_string(pieces)
                num_order_in_file = [] if not pieces else [item.split("_")[-1][5:] for item in pieces]
                fetch_file(sock, peers_port, file_name, pieces_hash, num_order_in_file)
            elif user_input.lower() == 'exit':
                send_status_update(sock, peers_port, 'stopped', "")
                print("something in exit command")
                stop_event.set()
                sock.close()
                break
            else:
                print("Invalid command.")
    except Exception as e:
        print(f"error in nhập lệnh: {e}")
    finally:
        host_service_thread.join()

if __name__ == "__main__":
    SERVER_HOST = '192.168.56.1'
    SERVER_PORT = 65432
    CLIENT_PORT = 65431
    main(SERVER_HOST, SERVER_PORT, CLIENT_PORT)
