insert into medivh.sales__by_barcode_by_day
select sp.store_id,
       sp.barcode,
       str_to_date(concat(extract(year_month from sp.sale_time), extract(day from sp.sale_time)), '%Y%m%d') as date,
       sum(sp.quantity) as quantity
from medivh.sale__product sp
#      where sp.store_id > 56
group by sp.store_id, sp.barcode, concat(extract(year_month from sp.sale_time), extract(day from sp.sale_time));
