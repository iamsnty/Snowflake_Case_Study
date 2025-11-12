select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select c_custkey
from DBT_CI_DEMO.SRC.src_customers
where c_custkey is null



      
    ) dbt_internal_test