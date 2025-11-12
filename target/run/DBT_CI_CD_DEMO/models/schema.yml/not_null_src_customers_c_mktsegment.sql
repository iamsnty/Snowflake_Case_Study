select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select c_mktsegment
from DBT_CI_DEMO.SRC.src_customers
where c_mktsegment is null



      
    ) dbt_internal_test