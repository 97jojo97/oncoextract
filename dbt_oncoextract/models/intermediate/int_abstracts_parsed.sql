with abstracts as (
    select * from {{ ref('stg_pubmed_abstracts') }}
),

biomarker_flags as (
    select
        abstract_id,
        pmid,
        title,
        abstract_text,
        publication_date,
        publication_year,
        journal,
        mesh_terms,
        abstract_length,

        -- Cancer subtype detection from MeSH terms
        case
            when mesh_terms::text ilike '%nasopharyngeal%' then 'Nasopharyngeal Carcinoma'
            when mesh_terms::text ilike '%lung neoplasm%' then 'Lung Cancer'
            when mesh_terms::text ilike '%breast neoplasm%' then 'Breast Cancer'
            else 'Other/Unspecified'
        end as cancer_type,

        -- Biomarker mentions in abstract text
        case when abstract_text ilike '%EBV%'
              or abstract_text ilike '%Epstein-Barr%'
              or abstract_text ilike '%EBV DNA%'
            then true else false
        end as mentions_ebv,

        case when abstract_text ilike '%PD-L1%'
              or abstract_text ilike '%programmed death-ligand%'
            then true else false
        end as mentions_pdl1,

        case when abstract_text ilike '%p53%'
              or abstract_text ilike '%TP53%'
            then true else false
        end as mentions_p53,

        -- Treatment modality detection
        case when abstract_text ilike '%chemotherapy%'
              or abstract_text ilike '%cisplatin%'
              or abstract_text ilike '%carboplatin%'
              or abstract_text ilike '%gemcitabine%'
            then true else false
        end as mentions_chemotherapy,

        case when abstract_text ilike '%radiation%'
              or abstract_text ilike '%radiotherapy%'
              or abstract_text ilike '%IMRT%'
            then true else false
        end as mentions_radiation,

        case when abstract_text ilike '%immunotherapy%'
              or abstract_text ilike '%checkpoint inhibitor%'
              or abstract_text ilike '%pembrolizumab%'
              or abstract_text ilike '%nivolumab%'
            then true else false
        end as mentions_immunotherapy

    from abstracts
)

select * from biomarker_flags
