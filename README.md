# spark-ddm
assignment for data design and modelling on spark

# Steps to Install
1. ```shell 
   python3 -m venv ./.venv```
2. ```shell 
    source .venv/bin/activate```
3. ```shell
    pip install -r requirement.txt```
   
# Run
## Project queries:
In file ```main_query.py``` are the query for the exercise, 
in the main function, uncomment the lines to chose what to run
## Bonus:
The file ```main.py``` is the starting point for the bonus,
run the main function to start the server, then go to ```http://127.0.0.1:3000```,
this will open the main page. from here you can type in the search bar, then
either search by title or by author. In the newly opened page you will find a list
of books if any was found, by clicking the button for the reviews another page
will open with all the reviews of the selected book.