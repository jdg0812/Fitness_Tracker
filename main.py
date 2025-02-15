from flask import Flask, render_template
import sqlite3
import pandas as pd


app = Flask(__name__)

@app.route("/")
def home():
    conn = sqlite3.connect('strava_activities.db')
    cursor = conn.cursor()
    cursor.execute("""Select type, count(*) from activities group by type""")
    activites = cursor.fetchall()
    conn.close()

    return render_template('home.html', activities = activites)



if __name__ == '__main__':
    app.run(debug=True)