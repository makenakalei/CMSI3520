import mechanicalsoup
import pandas as pd
import sqlite3

url = "https://en.wikipedia.org/wiki/Wes_Anderson_filmography"
browser = mechanicalsoup.StatefulBrowser()
browser.open(url)


td = browser.page.find_all("td")
columns = [value.text.replace("\n", "") for value in td]
# print(columns)


columns = columns[0:55]
print(columns)

column_names = [ "Year",
                "Title",
                "Director",
                "Writer",
                "Producer"]

# column[0:][::11]
# column[1:][::11]
# column[2:][::11]

# dictionary = {"Year": year}
dictionary = {}
for idx, key in enumerate(column_names):
    dictionary[key] = columns[idx:][::10]
print(len(dictionary))
print(len(dictionary['Year']))
print(len(dictionary['Title']))


df = pd.DataFrame(data = dictionary)
print(df.head())
print(df.tail())

connection = sqlite3.connect("wesanderson.db")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS wes_anderson_movies")
cursor.execute("create table wes_anderson_movies (Year, " + ",".join(column_names[1::]) + ")")
for i in range(len(df)):
    cursor.execute("insert into wes_anderson_movies values (?,?,?,?,?)", df.iloc[i])

connection.commit()

connection.close()