{#-
  Managed counterpart to audience_interest_embeddings.py: the same texts,
  embedded by a Model Serving endpoint through ai_query().

  DISABLED unless the `embedding_endpoint` var (DBT_EMBEDDING_ENDPOINT) names an
  embedding endpoint, e.g. a Foundation Model API endpoint such as
  databricks-gte-large-en. Choosing between the two paths:
    * mapInPandas: you own the code and the dependencies; cost is compute time.
    * ai_query:    no code to own, and the endpoint scales on its own; cost is
                   per token, and the model is whatever the endpoint serves.
-#}
select
    member_id,
    interests_text,
    ai_query('{{ var("embedding_endpoint") }}', interests_text) as embedding
from {{ ref('audience_members_validated') }}
where is_valid
