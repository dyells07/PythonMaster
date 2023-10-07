import http.server
import socketserver
from urllib.parse import parse_qs

from recommendation import recommend_movies_for_user


PORT = 8000

class MovieRecommendationHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            with open('templates/index.html', 'rb') as file:
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                self.wfile.write(file.read())

    def do_POST(self):
        if self.path == '/recommend':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length).decode('utf-8')
            form_data = parse_qs(post_data)

            # Extract user data from the form
            user_data = {
                "age": int(form_data['age'][0]),
                "gender": int(form_data['gender'][0]),
                "occupation": int(form_data['occupation'][0]),
                "zipcode": form_data['zipcode'][0]
            }

            # Call your movie recommendation function with user data
            recommended_movies = recommend_movies_for_user(user_data)

            # Send the response with recommended movies
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write('<ul>'.encode('utf-8'))
            for movie in recommended_movies:
                self.wfile.write(f'<li>{movie}</li>'.encode('utf-8'))
            self.wfile.write('</ul>'.encode('utf-8'))

with socketserver.TCPServer(("", PORT), MovieRecommendationHandler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()
