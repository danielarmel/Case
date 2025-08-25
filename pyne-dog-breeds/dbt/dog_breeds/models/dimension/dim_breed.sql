{{ config(
    materialized='table'
) }}



with staging_data as (
    select * from {{ ref('stg_dog_api_raw') }}
),

-- Unpivot BredFor columns into rows
bred_for_unpivot as (
    select
        ID,
        Breed,
        BreedGroup,
        'BredFor' as AttributeType,
        AttributeValue,
        Weight,
        Height,
        LifeSpan
    from staging_data
    cross join unnest([
        BredFor1, BredFor2, BredFor3, BredFor4, BredFor5
    ]) as AttributeValue
    where AttributeValue is not null
),

-- Unpivot Temperament columns into rows
temperament_unpivot as (
    select
        ID,
        Breed,
        BreedGroup,
        'Temperament' as AttributeType,
        AttributeValue,
        Weight,
        Height,
        LifeSpan
    from staging_data
    cross join unnest([
        Temperament1, Temperament2, Temperament3, Temperament4, Temperament5
    ]) as AttributeValue
    where AttributeValue is not null
),

-- Unpivot Origin columns into rows
origin_unpivot as (
    select
        ID,
        Breed,
        BreedGroup,
        'Origin' as AttributeType,
        AttributeValue,
        Weight,
        Height,
        LifeSpan
    from staging_data
    cross join unnest([
        Origin1, Origin2, Origin3, Origin4, Origin5
    ]) as AttributeValue
    where AttributeValue is not null
),

-- Combine all unpivoted data
all_attributes as (
    select * from bred_for_unpivot
    union all
    select * from temperament_unpivot
    union all
    select * from origin_unpivot
),

-- Add unknown row
with_unknown as (
    select
        row_number() over (order by Breed, AttributeType, AttributeValue) as SK_Breed,
        ID,
        Breed,
        BreedGroup,
        AttributeType,
        AttributeValue
    from all_attributes
    
    union all
    
    select
        -1 as SK_Breed,
        -1 as ID,
        'Unknown' as Breed,
        'Unknown' as BreedGroup,
        'Unknown' as AttributeType,
        'Unknown' as AttributeValue
)
--test
select * from with_unknown