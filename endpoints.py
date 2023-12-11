from flask import make_response, render_template, request
from flask import Flask


app = Flask(__name__)


@app.route("/", method=["GET"])
def base_route():
    file = open("template.html", "r")
    data = file.read()
    file.close()
    # TODO make query and pass it to render template
    res = render_template(data)
    response = make_response(res)
    response.headers["Content-Type"] = "text/html"
    return response


@app.route("/books", method=["GET"])
def get_books_from_author():
    request.args.get("autor")
    pass

