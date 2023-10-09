import pandas as pd
import sqlite3

df = pd.read_csv("songs_normalize.csv")
print(df.head())
print(df.tail())


connection = sqlite3.connect("songs.db")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS songs")

df.to_sql("songs", connection)

connection.close()
