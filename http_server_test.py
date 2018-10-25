
#https://yiyibooks.cn/xx/python_352/library/http.server.html
#https://yiyibooks.cn/xx/python_352/library/socketserver.html#socketserver.TCPServer
from http.server import HTTPServer,BaseHTTPRequestHandler,SimpleHTTPRequestHandler

from http.client import HTTPResponse
import os

MEDIA_ROOT = "/home/docker"

class HttpRequest(object):
    pass

def parse_mime(path):
    _,ext = os.path.splitext(path)
    if ext == ".ico":
        mime = "image/x-icon"
    elif ext == ".ief":
        mime = "image/ief"
    elif ext == ".jfif":
        mime = "image/jpeg"
    elif ext == ".jpe" or ext == ".jpeg" or ext == ".jpg":
        mime = "image/jpeg"
    elif ext == ".mp4":
        mime = "audio/mp4"
    elif ext == ".mp3":
        mime = "audio/mp3"
    elif ext == ".pdf":
        mime = "application/octet-stream"
    elif ext == ".js":
        mime = "application/ecmascript"
    else:
        mime = "application/octet-stream"

class Handler(BaseHTTPRequestHandler):
    """def handle(self):
        print(self.request[0])
        调用handle_one_request()一次（如果启用持久连接，则多次）以处理传入的HTTP请求。你应该永远不需要覆盖它；而是实现适当的do_*()方法。
    """
    def do_GET(self):
        #简单的静态服务器
        path = self.path
        self.send_response(200)
        mime = parse_mime(path)
        self.send_header("Content-type", mime)
        self.end_headers()


        dir_path = os.path.join(MEDIA_ROOT,path.strip("/"))
        try:
            with open(dir_path,"rb") as f:

                self.wfile.write(f.read())
        except:
            with open("404.html","rb") as f:

                self.wfile.write(f.read())


    def do_POST(self):
        l = self.headers.get("content-length")

        print(self.rfile.read(int(l)))




if __name__ == "__main__":
    port = 8080
    host = "0.0.0.0"
    server = HTTPServer((host,port),Handler)
    server.serve_forever()