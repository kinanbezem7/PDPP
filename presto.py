import prestodb
import pandas as pd
connection = prestodb.dbapi.connect(
    host = "localhost",
    catalog = "cassandra",
    user = "kinan",
    port = 8080,
    schema = "ks"

)

cur = connection.cursor()
cur.execute("SELECT * FROM pintrest")
rows = cur.fetchall()

# for row in rows:
#     print(row)

pintrest_dataframe = pd.DataFrame(rows)
print(pintrest_dataframe)