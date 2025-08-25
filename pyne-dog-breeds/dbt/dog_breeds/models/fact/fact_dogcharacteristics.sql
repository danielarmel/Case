{{ config(
    materialized='table'
) }}

with base as (
    select
        *
    from {{ ref('stg_dog_api_raw') }}
),

parsed as (
    select
        Breed,
        -- Life Span: handle "11 – 14 years", "12 - 13 years", "12 years"
        case 
            when LifeSpan like '%-%' or LifeSpan like '%–%' then
                safe_cast(trim(split(replace(replace(LifeSpan, 'years', ''), '–', '-'), '-')[offset(0)]) as int64)
            else
                safe_cast(trim(replace(LifeSpan, 'years', '')) as int64)
        end as MinLifeSpan,
        
        case 
            when LifeSpan like '%-%' or LifeSpan like '%–%' then
                coalesce(
                    safe_cast(trim(split(replace(replace(LifeSpan, 'years', ''), '–', '-'), '-')[safe_offset(1)]) as int64),
                    safe_cast(trim(split(replace(replace(LifeSpan, 'years', ''), '–', '-'), '-')[offset(0)]) as int64)
                )
            else
                safe_cast(trim(replace(LifeSpan, 'years', '')) as int64)
        end as MaxLifeSpan,

-- Weight: handle both regular hyphen and en dash
        case 
            when Weight like '%-%' or Weight like '%–%' then
                safe_cast(trim(split(replace(Weight, '–', '-'), '-')[offset(0)]) as float64)
            else
                safe_cast(trim(Weight) as float64)
        end as MinWeightKg,
        
        case 
            when Weight like '%-%' or Weight like '%–%' then
                coalesce(
                    safe_cast(trim(split(replace(Weight, '–', '-'), '-')[safe_offset(1)]) as float64),
                    safe_cast(trim(split(replace(Weight, '–', '-'), '-')[offset(0)]) as float64)
                )
            else
                safe_cast(trim(Weight) as float64)
        end as MaxWeightKg,

        -- Height: handle both regular hyphen and en dash
        case 
            when Height like '%-%' or Height like '%–%' then
                safe_cast(trim(split(replace(Height, '–', '-'), '-')[offset(0)]) as float64)
            else
                safe_cast(trim(Height) as float64)
        end as MinHeightCm,
        
        case 
            when Height like '%-%' or Height like '%–%' then
                coalesce(
                    safe_cast(trim(split(replace(Height, '–', '-'), '-')[safe_offset(1)]) as float64),
                    safe_cast(trim(split(replace(Height, '–', '-'), '-')[offset(0)]) as float64)
                )
            else
                safe_cast(trim(Height) as float64)
        end as MaxHeightCm

    from base
),

-- Get BredFor from dim_breed
bred_for_dim as (
    select distinct
        ID,
        Breed,
        AttributeValue as BredFor
    from {{ ref('dim_breed') }}
    where AttributeType = 'BredFor'
),

-- Get Origin from dim_breed
origin_dim as (
    select distinct
        ID,
        Breed,
        AttributeValue as Origin
    from {{ ref('dim_breed') }}
    where AttributeType = 'Origin'
),

-- Get Temperament from dim_breed
temperament_dim as (
    select distinct
        ID,
        Breed,
        AttributeValue as Temperament
    from {{ ref('dim_breed') }}
    where AttributeType = 'Temperament'
),

joined as (
    select distinct
        coalesce(d.SK_Breed, -1) as SK_Breed,
        p.MinLifeSpan,
        p.MaxLifeSpan,
        p.MinWeightKg,
        p.MaxWeightKg,
        p.MinHeightCm,
        p.MaxHeightCm
    from parsed p
    left join bred_for_dim bf on p.Breed = bf.Breed 
    left join origin_dim o on p.Breed = o.Breed 
    left join temperament_dim t on p.Breed = t.Breed 
    left join {{ ref('dim_breed') }} d on p.Breed = d.Breed
)

select * from joined