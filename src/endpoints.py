from flask import make_response, render_template, request
from flask import Flask, Blueprint

templates = Blueprint('templates', __name__, template_folder='templates')
app = Flask(__name__)
app.register_blueprint(templates)


@app.route("/", methods=["GET"])
def base_route():
    res = render_template("template.html")
    response = make_response(res)
    response.headers["Content-Type"] = "text/html"
    return response


@app.route("/books", methods=["GET"])
def get_books_from_author():
    author = request.args.get("author")
    print(f"author is: {author}")
    # TODO: render the template
    return "OK"


@app.route("/reviews", methods=["GET"])
def get_reviews_from_author():
    book = request.args.get("book")
    print(f"book is: {book}")
    # TODO: render the template
    return "OK"

