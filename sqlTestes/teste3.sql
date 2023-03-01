Select
    review_score,
    avg(price) as mean_price,
    avg(freight_value) as mean_freight,
    avg(price_total) as mean_price_total,
    avg(payment_value) as mean_payment_value
from dl_rocessing_zone.aggregate
where customer_state in ('SP','MG','RJ')
group by review_score
order by review_score DESC