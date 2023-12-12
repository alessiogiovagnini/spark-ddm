from flask import make_response, render_template, request
from flask import Flask
import flask
from src.query import get_book_info

app = Flask(__name__, template_folder="../templates")


@app.errorhandler(404)
def page_not_found(e):
    return "Not found"


@app.errorhandler(400)
def bad_request(e):
    return "Bad request"


@app.route("/", methods=["GET"])
def base_route():
    res = render_template("index.html")
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
    book_title = request.args.get("book")

    return "OK"


@app.route("/info", methods=["GET"])
def get_books_info():
    book_title = request.args.get("title")
    if not book_title:
        return flask.redirect("/400")

    info = get_book_info(book=book_title)

    res = render_template("book_template.html", rows=info)
    response = make_response(res)
    response.headers["Content-Type"] = "text/html"
    return response


