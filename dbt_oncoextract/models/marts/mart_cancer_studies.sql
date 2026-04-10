with parsed as (
    select * from {{ ref('int_abstracts_parsed') }}
)

select
    publication_year,
    cancer_type,
    journal,
    count(*) as study_count,
    avg(abstract_length) as avg_abstract_length,
    sum(case when mentions_ebv then 1 else 0 end) as ebv_studies,
    sum(case when mentions_pdl1 then 1 else 0 end) as pdl1_studies,
    sum(case when mentions_p53 then 1 else 0 end) as p53_studies,
    sum(case when mentions_chemotherapy then 1 else 0 end) as chemo_studies,
    sum(case when mentions_radiation then 1 else 0 end) as radiation_studies,
    sum(case when mentions_immunotherapy then 1 else 0 end) as immunotherapy_studies
from parsed
where publication_year is not null
group by publication_year, cancer_type, journal
order by publication_year desc, study_count desc
