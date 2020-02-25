import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+mysqlconnector://root:root@localhost/medivh')

df = pd.read_sql('select date, quantity from sales__by_day where barcode = 5449000133328', con=engine)
print(df)

