from flask import make_response, render_template, request
from flask import Flask
import flask
from src.query import get_book_info, get_book_reviews, get_books_from_author

app = Flask(__name__, template_folder="../templates")


@app.errorhandler(404)
def page_not_found(e):
    return f"Not found: {e}"


@app.errorhandler(400)
def bad_request(e):
    return f"Bad request: {e}"


@app.route("/", methods=["GET"])
def base_route():
    res = render_template("index.html")
    response = make_response(res)
    response.headers["Content-Type"] = "text/html"
    return response


# get all books info that are written by the same author
@app.route("/books", methods=["GET"])
def get_books_from_author_api():
    author = request.args.get("author")
    if not author:
        return flask.redirect("/400")
    info = get_books_from_author(author=author)

    res = render_template("book_template.html", rows=info)
    response = make_response(res)
    response.headers["Content-Type"] = "text/html"
    return response


# get all reviews from a book title
@app.route("/reviews", methods=["GET"])
def get_reviews_from_author():
    book_title = request.args.get("title")
    if not book_title:
        return flask.redirect("/400")
    reviews = get_book_reviews(book_title=book_title)

    res = render_template("review_template.html", rows=reviews, title=book_title)
    response = make_response(res)
    response.headers["Content-Type"] = "text/html"
    return response


# get information on all books with this title
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


