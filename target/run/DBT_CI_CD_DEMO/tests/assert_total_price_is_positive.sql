select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Check if the total price is negetive.
select orders.o_totalprice
from DBT_CI_DEMO.SRC.src_orders as orders
where orders.o_totalprice < 0
      
    ) dbt_internal_test