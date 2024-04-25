import grpc
import time
from concurrent import futures
import sqlite3
import user_pb2
import user_pb2_grpc

class UserService(user_pb2_grpc.UserServiceServicer):
    def __init__(self, db_path='users.db'):
        self.db_path = db_path


        
    def create_table(self):
        cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                            id TEXT PRIMARY KEY,
                            name TEXT,
                            email TEXT)''')
        self.conn.commit()




    def GetUser(self, request, context):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, email FROM users WHERE id = ?", (request.user_id,))
        user_data = cursor.fetchone()
        if user_data:
            user = user_pb2.User(id=user_data[0], name=user_data[1], email=user_data[2])
            return user_pb2.GetUserResponse(user=user)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("User not found")
            return user_pb2.GetUserResponse()


class CrudUserService(user_pb2_grpc.CrudUserServicer):
    def __init__(self, db_path='users.db'):
        self.db_path = db_path

    def create_table(self, cursor):
        cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                            id TEXT PRIMARY KEY,
                            name TEXT,
                            email TEXT)''')




    def CreateUser(self, request, context):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        self.create_table(cursor)
        cursor.execute("INSERT INTO users (id, name, email) VALUES (?, ?, ?)", (request.id, request.name, request.email))
        conn.commit()
        return request


    def UpdateUser(self, request, context):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET name = ?, email = ? WHERE id = ?", (request.name, request.email, request.id))
        conn.commit()
        return request




    def DeleteUser(self, request, context):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name, email FROM users WHERE id = ?", (request.user_id,))
        deleted_user = cursor.fetchone()
        name, email=deleted_user
        cursor.execute("DELETE FROM users WHERE id = ?", (request.user_id,))
        conn.commit()
        return user_pb2.User(id=request.user_id, name=name, email=email)

    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    user_pb2_grpc.add_CrudUserServicer_to_server(CrudUserService(), server)
    server.add_insecure_port('localhost:50051')
    server.start()
    print("Server started, listening on port 50051...")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
