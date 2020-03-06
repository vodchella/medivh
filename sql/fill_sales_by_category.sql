insert into medivh.sales__by_category_by_day
select sd.store_id,
       p.category_id,
       sd.date,
       sum(sd.quantity) as quantity
from   medivh.sales__by_barcode_by_day sd
       inner join medivh.store    s on (s.id = sd.store_id)
       inner join medivh.product  p on (p.barcode = sd.barcode and p.store_group_id = s.store_group_id)
group by sd.store_id, sd.date, p.category_id;
