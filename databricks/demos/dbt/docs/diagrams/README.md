# Diagrams (draft) — The One True Way

Self-contained, browser-openable HTML visuals for [`../../THE_ONE_TRUE_WAY.md`](../../THE_ONE_TRUE_WAY.md).
Open any `.html` file locally in a browser (no assets/CDNs — works offline). These are **drafts**;
shapes are still being refined for final publishing.

Each was designed pain-first — start from "what's hard to grasp in prose," then design the shape that kills it.

| file | concept | the pain it kills |
|------|---------|-------------------|
| `01-deployment-lifecycle.html` | **One model, three environments** (flagship) | the dev/CI/prod routing rule is invisible in a table — you have to run the macro in your head 3× |
| `02-zero-config-resolution.html` | Zero-config, except the 2 required vars | can't tell what works on `dbt build` out-of-the-box vs what errors |
| `03-dev-sandboxes.html` | Per-developer sandboxes (confidence vs hesitation) | "everyone builds the same models but never collides" is abstract |
| `04-ephemeral-ci-schema.html` | Disposable per-PR CI schemas | "a re-run builds separately from the last attempt" is a temporal story |
| `05-cost-lineage.html` | Cost-analytics lineage | the README's ASCII tree is flat — no materializations, no incremental fact, no join |
| `06-git-lifecycle-environments.svg` | Git lifecycle → service principals → catalogs (embedded in the main README) | who runs what, and where it lands — two SPs, three catalogs, and the release gate assembled from prose means holding all of it in your head |
| `scheduled-builds-migration.html` | Scheduled builds before and after migration | schedule, state, and docs ownership are difficult to compare across two control planes |
| `slim-ci-migration.html` | Slim CI before and after migration | state deferral, schema isolation, and cleanup form one CI contract |
| `state-based-cd-migration.html` | State-based CD before and after migration | deployment and state handoff must form one reviewable release path |
| `dbt-version-ownership-migration.html` | dbt adapter-version ownership before and after migration | a compatibility floor and an executable pin solve different problems |

`06` is a hand-authored SVG (renders directly on GitHub, no build step): each row is one
environment, colored end-to-end — the branch, the principal it runs as, and the catalog it
lands in. Edit the SVG directly; there is no generator source.

Style: "Mono Bold" — white canvas, black monospace, thin borders, neon accent + glow on the one node each diagram is really about.

The four migration diagrams use the Brief theme. Each HTML source includes its exact
2× render command and sits beside its PNG.
