select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      

    with meet_condition as(
    select *
    from normalize.ew_insurance.__policycategory
    ),

    validation_errors as (
    select *
    from meet_condition
    where
   
        appcount < 0
    
        or  appcount > 1
    )

    select *
    from validation_errors
    )
