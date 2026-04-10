with parsed as (
    select * from {{ ref('int_abstracts_parsed') }}
)

select
    publication_year,
    cancer_type,

    count(*) as total_abstracts,

    -- Treatment distribution
    sum(case when mentions_chemotherapy then 1 else 0 end) as chemo_count,
    sum(case when mentions_radiation then 1 else 0 end) as radiation_count,
    sum(case when mentions_immunotherapy then 1 else 0 end) as immunotherapy_count,
    sum(case when mentions_chemotherapy and mentions_radiation then 1 else 0 end) as chemoradiation_count,

    -- Treatment percentages
    round(
        100.0 * sum(case when mentions_chemotherapy then 1 else 0 end) / nullif(count(*), 0), 1
    ) as chemo_pct,
    round(
        100.0 * sum(case when mentions_radiation then 1 else 0 end) / nullif(count(*), 0), 1
    ) as radiation_pct,
    round(
        100.0 * sum(case when mentions_immunotherapy then 1 else 0 end) / nullif(count(*), 0), 1
    ) as immunotherapy_pct,

    -- Biomarker co-occurrence
    sum(case when mentions_ebv then 1 else 0 end) as ebv_mentions,
    sum(case when mentions_pdl1 then 1 else 0 end) as pdl1_mentions

from parsed
where publication_year is not null
group by publication_year, cancer_type
order by publication_year desc, total_abstracts desc
