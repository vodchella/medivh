import pandas as pd
from pandas import DataFrame
from pkg.utils.df import create_df_indexed_by_date
from sqlalchemy import create_engine as sqlalchemy_create_engine


def create_engine(db):
    db_path = '%s:%s@%s:%s/%s' % (db['user'], db['pass'], db['host'], db['port'], db['schema'])
    db_secured_path = '%s:%s@%s:%s/%s' % (db['user'], '*****', db['host'], db['port'], db['schema'])
    engine = sqlalchemy_create_engine(f'mysql+mysqlconnector://{db_path}')
    print(f'Connected to MySQL at {db_secured_path}\n')
    return engine


def get_barcode_daily_sales(engine, store_id: int, code: int) -> DataFrame:
    data = pd.read_sql(f'select date as date_idx, '
                       f'       quantity '
                       f'from   medivh.sales__by_barcode_by_day '
                       f'where  barcode = {code} and store_id = {store_id}', con=engine)
    return create_df_indexed_by_date(data)


def get_category_daily_sales(engine, store_id: int, code: int) -> DataFrame:
    data = pd.read_sql(f'select sc.date as date_idx, '
                       f'       sc.quantity '
                       f'from   medivh.sales__by_category_by_day sc '
                       f'where  sc.store_id = {store_id} and '
                       f'       sc.category_id = (select p.category_id '
                       f'                         from   medivh.product p '
                       f'                         where  p.barcode = {code} and '
                       f'                                p.store_group_id = (select s.store_group_id '
                       f'                                                    from   medivh.store s '
                       f'                                                    where  s.id = sc.store_id)) ', con=engine)
    return create_df_indexed_by_date(data)
