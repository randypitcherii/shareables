{% docs ai_ml_inference_overview %}
**Batch inference from a model logged in Unity Catalog, two ways.** dbt applies
the model. It never trains or registers one.

![dbt applies one Unity Catalog model two ways, at batch time](docs/diagrams/ai-ml-inference.png)

- **Path A**, `ai_ml_inference_embeddings_python`: a dbt Python model loads
  `models:/<name>/<version>` inside `mapInPandas` on serverless compute.
- **Path B**, `ai_ml_inference_embeddings_serving`: `ai_query()` on the SQL
  warehouse calls a Model Serving endpoint that serves the same version.
- `assert_ai_ml_inference_paths_agree` proves both give the same embedding
  for every row.

Details and how to choose: `models/ai_ml_inference/README.md`.
{% enddocs %}
