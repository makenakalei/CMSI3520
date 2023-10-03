import mechanicalsoup
import pandas as pd
import sqlite3

url = "https://en.wikipedia.org/wiki/Wes_Anderson_filmography"
browser = mechanicalsoup.StatefulBrowser()
browser.open(url)

th = browser.page.find_all("th")
year = [value.text.replace("\n", "") for value in th]
year = year[:5]
print("year" )
print(year)

td = browser.page.find_all("td")
columns = [value.text.replace("\n", "") for value in td]
# print(columns)


columns = columns[0:54]
print(columns)

column_names = [
                "Title",
                "Director",
                "Writer",
                "Producer"]

# column[0:][::11]
# column[1:][::11]
# column[2:][::11]

dictionary = {"Year": year}

for idx, key in enumerate(column_names):
    dictionary[key] = columns[idx:][::11]
print(len(dictionary))
print(len(dictionary['Year']))
print(len(dictionary['Title']))


df = pd.DataFrame(data = dictionary)
print(df.head())
print(df.tail())

connection = sqlite3.connect("wesanderson.db")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS wes_anderson_movies")
cursor.execute("create table wes_anderson_movies (Year, " + ",".join(column_names) + ")")
for i in range(len(df)):
    cursor.execute("insert into wes_anderson_movies values (?,?,?,?,?)", df.iloc[i])

connection.commit()

connection.close()