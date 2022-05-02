{{ config(materialized='view') }}

with ph_crops as (
    select *
    from {{ ref('PH_Beschreibung_Phasendefinition_Sofortmelder_Landwirtschaft_Kulturpflanze') }}
),

ph_fruit as (
    select *
    from {{ ref('PH_Beschreibung_Phasendefinition_Sofortmelder_Obst') }}
),

ph_wild as (
    select *
    from {{ ref('PH_Beschreibung_Phasendefinition_Sofortmelder_Wildwachsende_Pflanze') }}
),

ph_description_en as (
    select 
        Phase_ID as phase_id,
        Phase as phase,
        Phase_englisch as phase_en
    from {{ ref('PH_Beschreibung_Phase') }}
),

ph_unioned as (
    select * from ph_crops
    union all
    select * from ph_fruit
    union all
    select * from ph_wild
),

ph_unioned_renamed as (
    select
        Objekt_id as plant_id,
        Objekt as plantname_ger,
        Phasen_id as phase_id,
        Phase as phase,
        Phasendefinition as phase_definition,
        BBCH_Code as bbch_code,
        Hinweis_BBCH as bbch_note
    from ph_unioned
),

plant_description as (
    select 
        Objekt_ID as plant_id,
        Objekt as plantname_ger,
        Objekt_englisch as plantname_en,
        Objekt_latein as plantname_latin
     from {{ ref('PH_Beschreibung_Pflanze') }}
)


select 
    ph_unioned_renamed.plant_id,
    ph_unioned_renamed.plantname_ger,
    plantname_en,
    plantname_latin,
    ph_unioned_renamed.phase_id,
    ph_unioned_renamed.phase,
    phase_en,
    phase_definition,
    bbch_code,
    bbch_note
from ph_unioned_renamed
left join ph_description_en
    on ph_unioned_renamed.phase_id = ph_description_en.phase_id
left join plant_description
    on ph_unioned_renamed.plant_id = plant_description.plant_id
order by ph_unioned_renamed.plant_id, ph_unioned_renamed.phase_id 
