with source as (
    select
        pmid,
        title,
        abstract_text,
        authors,
        pub_date,
        mesh_terms,
        journal,
        cleaned_at
    from {{ source('public', 'cleaned_abstracts') }}
)

select
    pmid as abstract_id,
    pmid,
    title,
    abstract_text,
    authors,
    pub_date as publication_date,
    extract(year from pub_date) as publication_year,
    mesh_terms,
    journal,
    length(abstract_text) as abstract_length,
    cleaned_at
from source
where abstract_text is not null
  and length(abstract_text) > 10
