{{ config(
    materialized = 'table'
) }}

with base as (
    select * from {{ source('bronze', 'dog_api_raw') }}
),

-- test
renamed as (
    select
        id as ID,
        name as Breed,
        breed_group as BreedGroup,
        bred_for as BredFor,
        origin as Origin,
        temperament as Temperament,
        weight_metric as Weight,
        height_metric as Height,
        life_span as LifeSpan
    from base
)

select
    ID,
    Breed,
    BreedGroup,
    -- Split BredFor into separate columns (safely) with capital letters
    case when array_length(split(BredFor, ',')) > 0 then initcap(trim(split(BredFor, ',')[safe_offset(0)])) else null end as BredFor1,
    case when array_length(split(BredFor, ',')) > 1 then initcap(trim(split(BredFor, ',')[safe_offset(1)])) else null end as BredFor2,
    case when array_length(split(BredFor, ',')) > 2 then initcap(trim(split(BredFor, ',')[safe_offset(2)])) else null end as BredFor3,
    case when array_length(split(BredFor, ',')) > 3 then initcap(trim(split(BredFor, ',')[safe_offset(3)])) else null end as BredFor4,
    case when array_length(split(BredFor, ',')) > 4 then initcap(trim(split(BredFor, ',')[safe_offset(4)])) else null end as BredFor5,
    -- Split Temperament into separate columns (safely)
    case when array_length(split(Temperament, ',')) > 0 then trim(split(Temperament, ',')[safe_offset(0)]) else null end as Temperament1,
    case when array_length(split(Temperament, ',')) > 1 then trim(split(Temperament, ',')[safe_offset(1)]) else null end as Temperament2,
    case when array_length(split(Temperament, ',')) > 2 then trim(split(Temperament, ',')[safe_offset(2)]) else null end as Temperament3,
    case when array_length(split(Temperament, ',')) > 3 then trim(split(Temperament, ',')[safe_offset(3)]) else null end as Temperament4,
    case when array_length(split(Temperament, ',')) > 4 then trim(split(Temperament, ',')[safe_offset(4)]) else null end as Temperament5,
    -- Split Origin into separate columns (safely)
    case when array_length(split(Origin, ',')) > 0 then trim(split(Origin, ',')[safe_offset(0)]) else null end as Origin1,
    case when array_length(split(Origin, ',')) > 1 then trim(split(Origin, ',')[safe_offset(1)]) else null end as Origin2,
    case when array_length(split(Origin, ',')) > 2 then trim(split(Origin, ',')[safe_offset(2)]) else null end as Origin3,
    case when array_length(split(Origin, ',')) > 3 then trim(split(Origin, ',')[safe_offset(3)]) else null end as Origin4,
    case when array_length(split(Origin, ',')) > 4 then trim(split(Origin, ',')[safe_offset(4)]) else null end as Origin5,
    -- Handle Weight with NaN replacement
     case 
        when Weight = 'NaN - 8'  then '0 - 8' WHEN Weight = 'NaN'  then '0' ELSE Weight
    end as Weight,
    -- Handle Height with NaN replacement
    case 
        when Height like '%NaN%' then '0'
        else Height 
    end as Height,
    LifeSpan
from renamed