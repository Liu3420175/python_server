
#https://yiyibooks.cn/xx/python_352/library/http.server.html
#https://yiyibooks.cn/xx/python_352/library/socketserver.html#socketserver.TCPServer

from http.server import HTTPServer,BaseHTTPRequestHandler,SimpleHTTPRequestHandler

from http.client import HTTPResponse
import mimetypes
import os

MEDIA_ROOT = "/home/liulonghua/service"


class HttpRequest(object):
    pass


class Handler(BaseHTTPRequestHandler):
    """def handle(self):
        print(self.request[0])
        调用handle_one_request()一次（如果启用持久连接，则多次）以处理传入的HTTP请求。你应该永远不需要覆盖它；而是实现适当的do_*()方法。
    """
    def do_GET(self):
        #简单的静态服务器
        path = self.path
        self.send_response(200)
        mime = mimetypes.read_mime_types(path)
        self.send_header("Content-type", mime)
        self.end_headers()

        dir_path = os.path.join(MEDIA_ROOT,path.strip("/"))
        if os.path.isdir(dir_path):
            html = '<!doctype html>' \
                   '<html lang="en">' \
                   '<head>' \
                   '</head>' \
                   '<body>' \
                   '<a>%(dir_path)s</a>' \
                   '%(detail)s' \
                   '</body>' \
                   '</html>'
            file_list = os.listdir(dir_path)
            p = ['<a href="%s"> %s</a>  <br>'%(path + '/' + one, one) for one in file_list]
            print('===',path)
            p = ''.join(p)
            html = html%{'dir_path': dir_path, 'detail': p}
            self.wfile.write(html.encode('utf-8'))
        else:
            try:
                with open(dir_path, "rb") as f:

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
