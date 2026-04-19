# Architecture Decision Records

## ADR-001 — Databricks + dbt as the main stack

**Context**
Project designed to demonstrate proficiency with the dominant tools 
in the data engineering market in 2026.

**Decision**
Databricks as the execution and storage platform (Delta Lake), 
dbt for Silver → Gold transformations.

**Reasons**
- Databricks and dbt are the two most in-demand tools on mid/senior 
  Data Engineer job offers in France
- Databricks natively provides Delta Lake, Spark, and an orchestration 
  engine (Workflows) — no additional tooling required
- dbt on Databricks is the most widespread pattern for analytical 
  transformations — it cleanly separates business logic (dbt SQL) 
  from ingestion logic (Python)

**Alternatives considered**
- AWS Glue + Redshift: rejected, less dominant on the French market
- Spark standalone + Airflow: set aside for now — this 
  combination will be the main constraint of a dedicated future project

**Note**
The stack choice is not driven by project constraints — the data volume 
and complexity do not strictly require it. It is a deliberate upskilling 
decision on market-dominant tools, independent of the actual technical 
need. In real conditions, stack selection would be driven by business 
and infrastructure constraints, not the other way around.

## ADR-002 — Incremental ingestion strategy vs full reload

**Context**  
The main data source is the SIRENE stock file (INSEE), updated monthly,
containing all French business establishments. Two ingestion strategies
were possible to keep the lakehouse up to date each month.

**Key figures**  
- 42.9M establishments in France in the full stock file (as of March 2026)
- 5M establishments in Auvergne-Rhône-Alpes (~11.6%)
- 1.7M active establishments in AURA
- ~36,500 monthly modifications in AURA via the SIRENE delta API

**Decision**  
Hybrid ingestion:
- One-shot initialization from two source files:
  - The full stock file (CSV zip) — current state of all establishments
  - The historical stock file (CSV zip) — all historical periods for establishments with more than one recorded period
  - Both files are downloaded from data.gouv.fr stable URLs pointing to the latest published version
- Monthly updates via the SIRENE API, filtering on the date of last treatment by INSEE, offset by one second from the last successful run

**Why two source files for initialization**  
The stock file only contains the current state of each establishment — 
one row per siret, no historical periods. The historical stock file 
provides all past periods for establishments that have had more than 
one state. Combining both files allows reconstructing a complete SCD2 
Silver table from the pipeline's launch date, covering the full 
historical record available from INSEE at initialization time.

**Date capping at initialization**  
INSEE may record periods with future start or end dates (e.g. anticipated 
nomenclature changes). To prevent future period boundaries in Silver 
that would cause inconsistencies during monthly batch processing, all 
dates are capped at the maximum last treatment date found in the stock 
file during initialization. This value represents the maximum known 
temporal boundary at that point in time.

**Reasons**  
- Full monthly reload = reprocessing 5M rows every run
- Incremental ingestion = reprocessing ~36,500 rows every run
- Ratio: 137x less data processed per monthly run
- The SIRENE API provides a precise filter on last modification date
  — no need to diff files to detect changes
- The stock file URLs on data.gouv.fr always point to the latest 
  published version — no manual URL configuration required

**Alternatives considered**  
- Full monthly reload from stock file: simpler to implement, no pipeline 
  state management needed — rejected because 137x less efficient
- Stock file initialization via API pagination: technically possible 
  but counterproductive — over 3M sirets with multiple periods would 
  require thousands of paginated API calls, significantly slower and 
  more complex than directly downloading the historical stock file

**Known limitation**  
The delta cursor is based on the date of last treatment by INSEE, which 
can be updated for both business changes and purely technical modifications 
with no business impact. This may cause unnecessary batch processing of 
establishments that have not meaningfully changed. The significance filter 
applied during Silver transformation mitigates this — only changes on 
tracked columns trigger a new Silver period.

**Design implication**  
The pipeline has a start date — historical analysis can only go back 
to the first batch. This makes early pipeline launch strategically important.

**Pipeline state management**  
The pipeline maintains a delta cursor in a JSON state file stored on a 
Unity Catalog Volume. This file also holds the circuit breaker flag 
(see ADR-006). The cursor is updated at the end of each successful 
monthly batch — ensuring only changes since the last successful run 
are retrieved on the next call.

## ADR-003 — Bronze → Silver → Gold architecture

**Context**  
Having chosen an incremental ingestion strategy (see ADR-002),
a storage architecture was needed to support the core business case
and ensure pipeline reliability.

**Decision**  
Medallion architecture: Bronze → Silver → Gold.
Bronze retention is conditional — a batch is deleted from Bronze 
immediately once successfully transformed into Silver.

**Why a medallion architecture**  
The combination of a data pipeline feeding a BI use case naturally 
calls for a medallion architecture — it provides standard separation 
of concerns between raw data transit (Bronze), historized and validated 
data (Silver), and business-ready aggregations (Gold). Each layer 
addresses a distinct responsibility that cannot be substituted by another.

**Why Bronze as a transit-only layer**  
The business case requires month-over-month trend analysis —
answering questions like "is this sector growing or declining over time?"
requires reconstructing establishment state at any past date.
SCD Type 2 is the pattern that enables this, by storing each state
with its validity period rather than overwriting the current record.

Since the SIRENE source is a SCD Type 1 table — each establishment has a
single current state record, with no built-in history — transforming it
into SCD Type 2 means Silver already captures the full historical record
from the moment the pipeline was launched.

Therefore, the Bronze layer's sole purpose is to hold raw API batches
between ingestion and Silver transformation, ensuring no batch is lost
in case of failure between the two steps. Once a batch is successfully
transformed into Silver, it is deleted from Bronze — it has no long-term
storage role and no analytical value that Silver cannot already provide.

**Layers**  
- Bronze — transit zone: Delta Lake table, append-only, partitioned by `batch_date`
- Silver — historical source of truth: SCD Type 2 table accumulating monthly state snapshots since pipeline launch
- Gold — business aggregations: set of dbt models producing sector/region trend indicators for business consumption

## ADR-004 — Delta Lake as the storage format

**Context**  
The choice of Databricks as the execution platform (see ADR-001) makes 
Delta Lake the default storage format — Databricks is built around Delta Lake 
and the two are deeply integrated. This ADR documents the validation that 
Delta Lake is compatible with the project's actual constraints, rather than 
justifying the choice from scratch.

**Constraints to validate**  
- **ACID transactions** : the SCD Type 2 transformation requires closing 
  an existing record and inserting a new state in a single atomic operation. 
  If the operation is only partially applied, Silver is corrupted. 
  The entire transformation must be fully committed or fully rolled back 
  as one unit.
- **Columnar format** : Silver → Gold transformations are aggregation-heavy 
  (sector/region counts over time) — columnar storage maximizes query 
  performance for this access pattern.
- **dbt compatibility** : dbt is used for Gold transformations — 
  the storage format must be supported by a mature dbt connector.

**Validation**  
- Atomicity at the individual statement level is guaranteed — a write that 
  fails mid-way is fully rolled back, preventing partial writes or corrupted files.
- Delta Lake is built on Parquet — natively columnar.
- `dbt-databricks` is the most mature and feature-complete dbt connector 
  for this stack.

Delta Lake satisfies all three constraints — and is the natural choice 
in a Databricks environment.

**Note**  
In a platform-agnostic context, Iceberg or Hudi would have been 
equally valid candidates. At the scale of this specific project, 
DuckDB + Parquet would even have been technically sufficient.
  
Also, Delta Lake provides more than strictly needed — time travel and versioning — but these are acceptable overhead given the storage 
volumes involved (~36,500 rows per monthly batch). Versioning retention 
is kept at default (30 days) as the marginal storage cost does not 
justify configuration overhead.

## ADR-005 — Unity Catalog Volumes as primary storage

**Context**  
A storage layer accessible from both Python and Spark was needed to host:
- The Bronze transit layer (temporary batch holding between API ingestion 
  and Silver transformation)
- Intermediate files during first_fetch initialization (stock files, 
  filtered Parquet exports)
- The pipeline state file (delta cursor and circuit breaker flag)

**Decision**  
Unity Catalog Volumes as the primary storage layer for all non-Delta 
files. Delta Lake tables (Silver, Gold, Bronze) are stored in the 
Unity Catalog managed storage.

**Reasons**  
- Unity Catalog Volumes are natively accessible from both Python 
  (standard file I/O) and Spark — no credentials or connector 
  configuration required
- No external dependency — everything lives within the Databricks 
  workspace, reducing operational complexity
- Free Edition provides sufficient capacity for the project's storage 
  requirements (validated empirically — see capacity validation below)

**Storage capacity validation**  
- Filtered AURA stock file (Parquet): ~350 Mo — deleted after first_fetch
- Historical stock file (Parquet, filtered): deleted after first_fetch
- Silver table: ~211 Mo at initialization (11M rows, SCD2)
- Silver monthly delta: ~10 Mo/month
- Gold tables: ~20 Mo
- Bronze transit: ~10 Mo max per batch, deleted once transformed
- Pipeline state file: negligible
- Total after 12 months: ~450-500 Mo — well within Free Edition limits

**Alternatives considered**  
- Cloudflare R2 (original choice): rejected after discovering that 
  Databricks Serverless Free Edition does not support External Locations 
  with access key / secret key credentials — only IAM role-based 
  authentication is supported, which is AWS-specific and incompatible 
  with R2
- AWS S3: rejected — free tier limited to 6 months
- DBFS (Databricks File System): rejected — deprecated in favor of 
  Unity Catalog Volumes, tight coupling to Databricks internal 
  implementation details

**Note**  
The original architecture (ADR-005 v1) was designed around Cloudflare R2. 
The switch to Unity Catalog Volumes was driven by a platform constraint 
discovered during implementation, not by a design preference. 
The decision to use an external object store remains architecturally 
valid for non-Serverless deployments or production-grade setups 
where storage isolation from the compute platform is a requirement.

## ADR-006 — Pipeline failure handling strategy

**Context**  
A failure handling strategy was needed across the entire pipeline to ensure:
- No batch is ever lost between ingestion and Silver transformation
- The pipeline recovers automatically from transient failures at the ingestion stage
- Logic failures are caught, isolated, and resolved without data loss 
  or Silver corruption
- The pipeline can be resumed after a human-triggered fix, without 
  requiring direct access to Databricks

**Failure handling by pipeline stage**  

*API → Bronze (ingestion)*  
- **Transient failure** (API down, network issue): handled by native 
  Databricks two-phase retry. If max retries reached, the run is 
  marked as failed and an alert is sent. The batch is not written to 
  Bronze — the next scheduled run will capture the missed changes via 
  the delta API filter, at the cost of a larger delta window.
- **Logic failure or unexpected failure**: the job exits with an alert. 
  Bronze is not written — no data loss risk at this stage since the 
  API remains queryable on the next run.

*Bronze → Silver (transformation)*  
- **No transient failure**: all operations at this stage are performed 
  within Unity Catalog (Delta Lake reads, writes, and deletes) — 
  no external dependency exists that could cause a transient failure.
- **Logic failure or unexpected failure**: a circuit breaker halts 
  the pipeline. The failing batch remains in Bronze, which acts as 
  the dead letter queue — no batch is ever deleted from Bronze unless 
  its Silver transformation has been verified as successful. The circuit 
  breaker is reset exclusively by a human operator via a GitHub Actions 
  manual trigger once the fix has been deployed (see ADR-007).
- **Atomicity**: the Silver transformation is performed as a single 
  atomic Delta MERGE — the full state change for all affected 
  establishments in a batch is committed in one operation. There is 
  no intermediate state where Silver is partially updated. If the MERGE 
  fails, Silver is unchanged.
- **Idempotence**: guaranteed by the MERGE semantics — replaying a 
  batch produces the same result regardless of how many times it is run.
- **Ordering guarantee**: when a batch fails, all subsequent batches 
  are queued — none are processed until the failing batch is resolved. 
  This enforces strict chronological ordering of Silver SCD Type 2 
  records, which is a hard requirement for trend analysis.

*Silver → Gold (dbt)*  
- **No transient failure**: all operations at this stage run within 
  Unity Catalog — no external dependency.
- **dbt model failure**: the failing Gold tables are rolled back to 
  their previous version via Delta time travel. Silver is untouched. 
  An alert is sent. The pipeline is not halted — Silver remains valid 
  and other Gold tables are unaffected.
- **Critical failure during rollback**: if the rollback itself fails 
  after a dbt failure, Gold tables may be in an inconsistent state. 
  An alert is sent with a clear indication that manual intervention 
  is required. The job exits without retry to avoid aggravating the 
  inconsistency.
- **Idempotence**: naturally guaranteed by the nature of dbt 
  aggregation models — replaying a dbt run always produces the same result.

**Bronze as the dead letter queue**  
The Bronze layer serves a dual purpose: transit zone for incoming 
batches, and implicit dead letter queue for failed transformations. 
A batch remains in Bronze until its Silver transformation is confirmed 
successful — at which point it is deleted. If the transformation fails, 
the batch stays in Bronze indefinitely until the circuit breaker is 
reset and the pipeline resumes. This design guarantees no batch is 
ever lost, without requiring a separate DLQ infrastructure.

**Circuit breaker**  
The circuit breaker is a flag stored in the pipeline state file on 
a Unity Catalog Volume. When set, the Bronze → Silver job exits 
immediately at startup without processing any batch. The flag is reset 
exclusively via a dedicated GitHub Actions manual trigger, which calls 
a reset job via the Databricks API. The reset job clears the flag and 
immediately triggers the Bronze → Silver job — resuming the pipeline 
without any direct access to Databricks required from the operator.

**Alternatives considered**  

*Schema enforcement at Bronze level:*  
Would reject any batch with an unexpected schema — simpler to implement 
but risks losing batches on INSEE schema changes. Rejected in favor of 
schema evolution at Bronze — no batch is worth losing over a schema 
change that the transformation layer can handle downstream.

*Direct Databricks access to reset the circuit breaker:*  
Rejected — couples the maintenance act to a specific technical skill 
and access level, inconsistent with the principle that maintenance 
requires no direct access to the orchestration layer.

*Auto-reset by code version change detection:*  
Would automatically reset the circuit breaker when a new code version 
is deployed. Rejected — no reliable way to confirm a new version 
actually fixes the failing batch. A deliberate human reset signal 
is an acceptable and safer tradeoff.

*Line-level quarantine:*  
Rejected — the business case is built on long-term trend analysis, 
not time-sensitive reporting. A full batch reprocess is acceptable 
in both compute and storage terms at monthly frequency. Partial batch 
processing would only make sense if the number of failing rows were 
small enough to produce a meaningful partial snapshot, which cannot 
be guaranteed.

## ADR-007 — Databricks jobs as the orchestration layer

**Context**  
The pipeline requires an orchestration layer capable of handling:
- Scheduled monthly ingestion
- Automatic retry on transient failures at the ingestion stage
- Circuit breaker pattern halting the pipeline on logic failures, 
  reset exclusively by a human operator
- Automatic pipeline resumption after a human-triggered reset, 
  without requiring direct access to Databricks
- Automatic Silver → Gold recalculation on dbt model changes

**Why Airflow is the best candidate on paper**  
Apache Airflow natively addresses all constraints above:
- Cron-based scheduling with fine-grained control
- Built-in retry with exponential backoff per task
- Sensor pattern — tasks that poll a condition and trigger downstream 
  tasks when the condition is met, which would natively handle circuit 
  breaker detection and pipeline resumption without custom implementation
- Native integration with external triggers and webhooks
- Large ecosystem, battle-tested in production data pipelines

Prefect and Dagster offer similar capabilities with more modern interfaces, 
but Airflow remains the most widely adopted in enterprise data engineering contexts.

**Why not Airflow**  
Introducing Airflow alongside Databricks would add a full orchestration 
stack to deploy, maintain, and secure — a significant operational overhead 
for a pipeline that runs monthly. The complexity of integrating two 
platforms outweighs the elegance of Airflow's native sensor pattern 
in this context.

Additionally, Databricks is the imposed platform for this project 
(see ADR-001). Databricks Jobs is capable of addressing all 
constraints without sacrificing pipeline quality or functionality — 
solving the orchestration problem within Databricks alone demonstrates 
deeper platform proficiency than delegating it to an external tool.

**How constraints are addressed with Databricks Jobs**  

*Retry strategy*  
Only the ingestion stage (API → Bronze) is subject to transient failures, 
as it depends on an external API (INSEE SIRENE). All downstream stages 
(Bronze → Silver, Silver → Gold) operate exclusively within Unity Catalog 
and have no external dependencies — transient failures are not a concern 
at these stages.

The ingestion workflow therefore implements a two-phase retry pattern:
- **Phase 1**: short retry — limited attempts, short interval. 
  Covers brief API outages or transient network issues.
- **Phase 2**: long retry — unlimited attempts, one-hour interval + alert. 
  Covers extended API unavailability (e.g. INSEE maintenance windows).

Phase 2 is triggered on failure of Phase 1 via Databricks Jobs 
conditional task execution. Both phases run the same job code — 
the retry behavior is configured at the job level, not in the code.

Bronze → Silver and Silver → Gold jobs use no retry — any failure 
is a logic or unexpected failure that requires human investigation, 
not automatic retry.

*Circuit breaker*  
The pipeline halt mechanism is implemented via a flag in the pipeline 
state file stored on a Unity Catalog Volume. When set, the Bronze → 
Silver job exits immediately at startup — no transformation is attempted. 
The flag is reset exclusively via a dedicated GitHub Actions manual 
trigger, which calls a reset job via the Databricks SDK. The reset job 
clears the flag and immediately triggers the Bronze → Silver job, 
resuming the pipeline without any direct Databricks access required 
from the operator.

*Inter-job triggering*  
Jobs are fully decoupled — each job is triggered independently:
- Ingestion → Bronze: triggered by monthly scheduler (10th of each month)
- Bronze → Silver: triggered by the ingestion job on success, or by 
  the GitHub Actions reset job on circuit breaker reset
- Silver → Gold: triggered by the Bronze → Silver job on success, or 
  by a GitHub Actions webhook on push to dbt model files

*Silver → Gold recalculation*  
A push to dbt model files in the repository triggers an automatic call 
to the Silver → Gold job via a GitHub Actions webhook, without 
requiring any human access to Databricks.

**Job and task structure**  
![Job diagram](./docs/job_architecture.png)  
*(see pipeline taks logic section below for task-level implementation)*  

  
**Pipeline task logic**  
═══════════════════════════════════════════════════════  
JOB : INGESTION → BRONZE  
═══════════════════════════════════════════════════════  

TASK IB-1 [Phase 1 — short retry: 10 attempts, 30 seconds interval]  
TASK IB-1a [Phase 2 — unlimited retry, 1 hour interval + alert]  
───────────────────────────────────────────────────────  
```
try:  
    If Silver table does not exist:
      Run first_fetch initialization
    Fetch API data filtered on last delta cursor date  
    Write to Bronze — MERGE on siret + batch_date for idempotence  
    Trigger Bronze→Silver Job  

except TransientFailure:  
    raise  → triggers retry  

except UnexpectedFailure:  
    Send critical alert — potential batch loss risk  
    raise  → triggers retry  
```

═══════════════════════════════════════════════════════  
JOB : BRONZE → SILVER  
═══════════════════════════════════════════════════════  

TASK BS-1 [no retry] 
───────────────────────────────────────────────────────  
```
try:
    If pipeline is halted → exit 0

    For each batch in Bronze ingestion queue, ordered chronologically:
        Build to_merge DataFrame:
          Load all Silver rows for affected sirets
          Apply significance filter — detect changes on tracked columns
          Apply fixed column updates across all periods
          Close open Silver rows where significant change detected
          Insert new open rows for changed sirets
          Insert rows for new sirets

        Run integrity checks on to_merge
        If integrity check fails:
          Set pipeline halted
          Send alert
          exit 0

        Merge to_merge into Silver (single atomic Delta MERGE)
        Delete processed Bronze batch

    Trigger Silver→Gold Job

except LogicFailure | UnexpectedFailure:
    Set pipeline halted
    Send alert
    exit 0
```

═══════════════════════════════════════════════════════  
JOB : SILVER → GOLD  
═══════════════════════════════════════════════════════  

TASK SG-1 [no retry]  
───────────────────────────────────────────────────────  
```
try:
    Capture current Gold versions for potential rollback

    Run dbt build  
    Record stage = "built" in pipeline state store

    If dbt failures:
      Restore failed Gold tables to captured versions
      Send alert with dbt failure report
      exit 0

except UnexpectedFailure:
    Send critical alert — manual Gold intervention required
    exit 0
```

## ADR-008 — Python modular scripts over notebooks  

**Context**  
Databricks supports two approaches for writing pipeline code: 
interactive notebooks and standard Python scripts. A choice was needed 
for the production pipeline codebase.

**Decision**  
Python modular scripts organized as a package, versioned in Git. 
Notebooks are reserved for exploration, debugging, and setup documentation.

**Reasons**  

*Testability*  
Python modules can be unit tested — functions are importable and 
testable in isolation. Notebooks cannot be unit tested without 
dedicated tooling that adds significant complexity.

*Clean versioning*  
Python scripts produce readable Git diffs — line-by-line changes 
are immediately understandable. Notebooks are serialized as JSON 
including outputs and metadata, producing noisy and largely unreadable 
diffs that make code review and change tracking impractical.

*Reusability*  
A function defined in a Python module is importable anywhere in the 
codebase — ingestion logic, transformation logic, and utilities can 
be shared across jobs without duplication. Notebook code cannot be 
imported directly.

*CI/CD compatibility*  
Python scripts can be linted, tested, and deployed automatically 
in a CI/CD pipeline. This enables automated quality checks on every 
push — syntax errors, style violations, and unused imports are caught 
before deployment.

*Maintainability*  
A modular structure with clear separation of concerns 
(ingestion, transformation, utils) makes the codebase easier to 
navigate, extend, and debug than a collection of notebooks.

**Where notebooks remain useful**  
- Interactive data exploration and prototyping
- Debugging pipeline issues interactively on live data
- Setup and onboarding documentation (e.g. `docs/setup_cluster.ipynb`)

## ADR-T — Technical implementation decisions

This ADR documents technical decisions made during implementation that 
are too granular for the architecture-level ADRs, but important enough 
to record for future maintainability and debugging.

---

### T-001 — start_at derivation logic

**Decision**  
The `start_at` of a Silver period is derived differently depending on 
the source of the period change:

- For periods sourced from the INSEE historical stock file 
  (multi-period establishments at initialization): `start_at = dateDebut`
- For single-period establishments at initialization: 
  `start_at = GREATEST(dateDebut, dateDernierTraitementEtablissement)`
- For monthly delta batches where only non-INSEE-historized columns 
  changed: `start_at = dateDernierTraitementEtablissement`
- For monthly delta batches where INSEE-historized columns changed 
  (with or without non-historized column changes): 
  `start_at = dateDebut` of the new INSEE period

**Rationale**  
`dateDebut` is updated when a field tracked by INSEE's own historization 
changes. `dateDernierTraitementEtablissement` is updated when a 
non-historized field changes. Taking the greatest of the two at 
initialization captures the most precise start date regardless of 
which field triggered the last change. For monthly deltas, `dateDebut` 
takes precedence when an INSEE-historized change is detected, as it 
represents the exact date the new state became effective according to INSEE.

---

### T-002 — end_at derivation logic

**Decision**  
The `end_at` of a Silver period being closed is derived as follows:

- For periods already closed in the INSEE historical stock file at 
  initialization: `end_at = dateFin` from the historical file
- For a period closed during a monthly delta due to a change in 
  non-INSEE-historized columns only: `end_at = dateDernierTraitementEtablissement`
- For a period closed during a monthly delta due to a change in 
  INSEE-historized columns (with or without non-historized changes): 
  `end_at = dateDebut` of the new INSEE period

**Rationale**  
When a non-historized column changes, the only available date reference 
is `dateDernierTraitementEtablissement` — INSEE does not provide a 
precise change date for these fields. When an INSEE-historized column 
changes, `dateDebut` of the new period is the exact date the previous 
state ended according to INSEE, and is therefore more precise.

---

### T-003 — Rétroactive modification assumption

**Decision**  
The pipeline assumes INSEE does not make retroactive modifications to 
`dateDernierTraitementEtablissement` for records already processed. 
The delta cursor is therefore set to the maximum last treatment date 
observed in the previous batch, offset by one second.

**Rationale**  
There is no reliable way to detect retroactive modifications without 
performing a full reload — which defeats the purpose of incremental 
ingestion. This assumption is documented and accepted. If INSEE performs 
a large-scale retroactive correction, the pipeline operator would need 
to perform a manual reinitialization. In practice, INSEE publishes 
corrections as new records with updated treatment dates, not as silent 
retroactive overwrites.

---

### T-004 — Single atomic MERGE as the Silver write pattern

**Decision**  
All Silver changes for a given batch — closing existing periods, 
inserting new periods, updating fixed columns across all historical 
rows — are applied in a single Delta MERGE operation. The full target 
state is constructed as an in-memory DataFrame before any write occurs.

**Rationale**  
Delta Lake does not guarantee atomicity across multiple statements. 
A sequence of UPDATE + INSERT + UPDATE could leave Silver in an 
inconsistent state if the job fails mid-sequence. A single MERGE 
is atomic by definition — either all changes are committed or none are. 
This eliminates the need for rollback logic at the Silver level.

---

### T-005 — Date capping at initialization

**Decision**  
During `first_fetch`, all period start and end dates are capped at 
the maximum `dateDernierTraitementEtablissement` found in the stock file.

**Rationale**  
INSEE may record periods with future `dateDebut` or `dateFin` values 
(e.g. anticipated nomenclature changes already registered in the system). 
A future `start_at` in Silver would cause `new_end_at < start_at` 
during the next monthly batch, producing an integrity check failure. 
Capping at the maximum known treatment date prevents this without 
discarding the affected records.

---

### T-006 — Fixed columns are not truly immutable

**Decision**  
Columns initially classified as immutable (postal code, commune code, 
commune label, Lambert coordinates) are treated as updateable across 
all historical Silver rows when a change is detected in a monthly batch.

**Rationale**  
During implementation, 2,032 cases of changes on these columns were 
observed in a single monthly batch. Likely causes include administrative 
commune mergers, INSEE address corrections, and coordinate recalculations. 
When a change is detected, it is applied to all historical Silver rows 
for the affected establishment — the assumption being that the correction 
applies retroactively to the entire history of the establishment's location.

---

### T-007 — Delta cursor offset by one second

**Decision**  
The delta cursor stored in the pipeline state is set to 
`MAX(dateDernierTraitementEtablissement) + 1 second` at the end of 
each successful batch.

**Rationale**  
Using the exact maximum date without offset would re-fetch the 
establishment with that exact treatment date on the next run, 
producing a non-empty batch even when no new changes have occurred. 
A one-second offset ensures the filter is strictly greater than the 
last processed date.

---

### T-008 — Significance filter on tracked columns

**Decision**  
A batch record triggers a new Silver period only if at least one column 
in the set of project-tracked historized columns has changed compared 
to the current open Silver row for that establishment.

**Rationale**  
INSEE updates `dateDernierTraitementEtablissement` for both business 
changes and purely technical modifications (nomenclature recodings, 
internal system updates) with no business impact. Without a significance 
filter, every such technical update would generate a new Silver period — 
inflating the table with meaningless rows and distorting trend analysis. 
Only changes on columns that carry business meaning for the project 
trigger a new period.

---

### T-009 — activitePrincipaleNAF25Etablissement follows activitePrincipaleEtablissement

**Decision**  
`activitePrincipaleNAF25Etablissement` (APE code in NAF2025 nomenclature) 
is treated as a project-historized column that follows 
`activitePrincipaleEtablissement` — meaning a change in either column 
triggers a new Silver period.

**Rationale**  
`activitePrincipaleNAF25Etablissement` is not historized by INSEE 
(no `dateDebut`/`dateFin` provided) and is not present in the historical 
stock file. It is a recodage of `activitePrincipaleEtablissement` into 
a newer nomenclature. In practice, both columns change together when 
the establishment's main activity changes. Treating `activitePrincipaleNAF25Etablissement` 
as a project-historized column ensures the NAF2025 code is always 
consistent with the APE code within each Silver period.

The known risk is that a mass INSEE recodage of NAF2025 codes — 
without any underlying business change — would generate spurious new 
Silver periods for all affected establishments. This risk is accepted 
and documented. INSEE publishes nomenclature changes in advance, 
allowing the operator to anticipate and handle them if needed.