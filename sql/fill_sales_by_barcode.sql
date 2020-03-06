insert into medivh.sales__by_barcode_by_day
select t.store_id,
       p.barcode,
       t.date,
       t.quantity
from (
         select sp.store_id,
                sp.product_id,
                str_to_date(concat(extract(year_month from sp.sale_time), extract(day from sp.sale_time)), '%Y%m%d') as date,
                sum(sp.quantity) as quantity
         from medivh.sale__product sp
         # where sp.store_id = 450
         group by sp.store_id, sp.product_id, concat(extract(year_month from sp.sale_time), extract(day from sp.sale_time))
     ) t
     inner join medivh.product  p on (p.id = t.product_id)
order by t.date;
