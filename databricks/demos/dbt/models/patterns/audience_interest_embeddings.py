"""Python model: a batch transform that is awkward in SQL, via mapInPandas.

Two patterns in one file:

1. **Lift a legacy stage, don't rewrite it.** `embed_texts` is copied UNCHANGED
   from an existing Python pipeline (see models/patterns/README.md for the
   before/after). Only the I/O changed: `spark.read.table(...)` became
   `dbt.ref(...)`, and `df.write.saveAsTable(...)` became `return df`. dbt now
   owns lineage, environment routing, tests, and docs for this stage, and the
   stage logic is still the code that already passed review.

2. **mapInPandas for per-batch Python.** Spark hands the function an iterator of
   pandas DataFrames (one per Arrow batch), so any pandas / numpy / scikit-learn
   code runs as-is, in parallel, with no Python row loop over the table. This is
   the self-hosted path for embeddings. The managed alternative is
   audience_interest_embeddings_ai_query.sql (a Model Serving endpoint via
   ai_query): less code to own, but it bills per token.

The "embedding" is a hashed character-trigram vector, a deterministic stand-in
for a real encoder that needs no model download. Swap `embed_texts` for a
sentence-transformer and the structure stays the same.
"""

import hashlib

import numpy as np
import pandas as pd

EMBEDDING_DIM = 64


# ---- lifted verbatim from the legacy pipeline ---------------------------------
def embed_texts(texts):
    """Hashed character-trigram embedding, L2-normalized, one row per text."""
    vectors = np.zeros((len(texts), EMBEDDING_DIM), dtype=np.float32)
    for i, text in enumerate(texts):
        padded = f"  {text or ''} "
        for j in range(len(padded) - 2):
            digest = int(hashlib.md5(padded[j : j + 3].encode()).hexdigest(), 16)
            sign = 1.0 if (digest >> 64) & 1 else -1.0
            vectors[i, digest % EMBEDDING_DIM] += sign
        norm = np.linalg.norm(vectors[i])
        if norm:
            vectors[i] /= norm
    return vectors


# ---- end of legacy code -------------------------------------------------------


def embed_batches(batches):
    for batch in batches:
        vectors = embed_texts(batch["interests_text"].tolist())
        yield pd.DataFrame(
            {
                "member_id": batch["member_id"],
                "interests_text": batch["interests_text"],
                "embedding": list(vectors),
            }
        )


def model(dbt, session):
    dbt.config(materialized="table")

    members = (
        dbt.ref("audience_members_validated")
        .filter("is_valid")
        .select("member_id", "interests_text")
    )
    return members.mapInPandas(
        embed_batches,
        schema="member_id bigint, interests_text string, embedding array<float>",
    )
