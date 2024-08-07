import http.server
import socketserver
import urllib.parse
import webbrowser

PORT = 8000

class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)
        if 'code' in params:
            auth_code = params['code'][0]
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(f"Authorization code: {auth_code}".encode())
            print(f"Authorization code: {auth_code}")
        else:
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"Waiting for authorization code...")

# Откройте браузер и перейдите по ссылке для получения кода авторизации
client_id = '172709f91e5747828bc4863e00745e6a'
auth_url = f"https://oauth.yandex.ru/authorize?response_type=code&client_id={client_id}"
webbrowser.open(auth_url)

# Запустите сервер для обработки перенаправления
with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()
