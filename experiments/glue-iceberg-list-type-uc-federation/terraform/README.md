# terraform/ — ephemeral AWS side of the experiment

Creates, in one AWS account and region:

| Resource | Purpose | Cost |
|---|---|---|
| S3 bucket (`<prefix>-<account>-<region>`) | Iceberg warehouse for both writers, UC foreign-catalog `storage_root`, Athena results | pennies (KB-scale tables) |
| Glue database | Registration target for both writer paths | free tier |
| IAM role (self-assuming) | UC **service credential** (Glue API) *and* **storage credential** (S3) in one | free |
| Athena workgroup | Cross-reader probe: does a non-Databricks Glue reader accept `list<...>`? | ~$5/TB scanned → effectively $0 |

Nothing here is long-lived. `make tf-destroy` removes all of it (`force_destroy`
is set on the bucket and workgroup so non-empty state does not block teardown).

## Trust policy

The role trusts the Unity Catalog master role with `sts:ExternalId = <your
Databricks account id>` **and** trusts itself (Databricks requires service and
storage credential roles to be self-assuming). The self-reference is an ARN
string built from the caller identity to avoid a Terraform cycle.

## Lifecycle

```
make tf-init
make tf-plan
make tf-apply     # prints an env snippet to paste into dev.env
make tf-destroy
```

After `tf-apply`, `make setup-uc` creates the Databricks-side objects
(service credential, storage credential, external location, Glue connection,
foreign catalog) from the outputs. Those are torn down by `make teardown-uc`.
