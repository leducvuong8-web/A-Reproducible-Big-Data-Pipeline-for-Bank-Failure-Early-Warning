# Data and phase handoffs

## Source inputs

Use Python 3.13.7 and, for local Spark, a JDK 17 or 21 environment with `JAVA_HOME` set. See the [PySpark installation documentation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html). Use the hash-pinned dependencies in the [project README](../README.md). Run from the repository root or its `notebooks/` directory. Before Phase 0, create `data/` at the repository root. Git does not track that directory, so it does not appear in a fresh checkout.

The submitted [research matrix](research_matrix.csv) contains 26 studies. The submitted FDIC schemas, financial-schema checksum, data dictionary, and exit-code lookup are in `docs/` alongside that matrix.

| File | Role |
|:--|:--|
| [fdic_financial_schema.yaml](fdic_financial_schema.yaml) and its [checksum metadata](fdic_financial_schema.yaml.meta.json) | Phase 0 verifies the URL and SHA-256 together before reusing the cached schema. Phase 1 verifies the schema against the Phase 0 handoff. Keep both from the same snapshot. |
| [data_dictionary.csv](data_dictionary.csv) | Phase 0 generates and refines this dictionary. A complete Phase 0 run rewrites it. |
| [fdic_exit_codebook.csv](fdic_exit_codebook.csv) | Phase 0 can reuse this cache of exit-code definitions. If absent, it queries FDIC History. |
| [fdic_failure_schema.yaml](fdic_failure_schema.yaml) and [fdic_institution_schema.yaml](fdic_institution_schema.yaml) | Reference definitions for the failures and institutions endpoints. The notebooks do not load these two YAML files by name. |

The notebooks acquire public records from the FDIC BankFind financials, failures, institutions, and history endpoints. The schema and dictionary files describe those records. The raw snapshots and derived Parquet files are kept locally. [FDIC API documentation](https://api.fdic.gov/banks/docs/)

## Start at the earliest phase for which you lack inputs

Run notebooks in order. Use separate kernels for Phases 0–3, then the same kernel for Phases 4 and 5. Reuse a saved handoff only with its corresponding artifacts from the same run.

| Phase | Inputs checked before use | Outputs required downstream |
|:--|:--|:--|
| 0 | `docs/research_matrix.csv`, a local `data/` directory, and FDIC API access for samples and reference retrieval. | `docs/phase0_artifact_bundle_manifest.json`, `phase0_data_scope_contract.json`, `phase0_phase1_field_handoff_v2.json`, `phase0_phase1_selected_fields_v2.csv`, `phase0_all_fdic_financial_fields_v2.csv`, and `phase0_ALL_field_scores_v2.csv`. |
| 1 | The complete Phase 0 bundle and the schema/research files it pins. | Quarterly Raw, annual Bronze under `data/bronze/financials/`, FDIC failures and institutions snapshots with their manifests, and the selected `outputs/phase1_from_phase0/runs/<run>/` with `reports/phase1_to_phase2_handoff.json`. Production requires `RUN_SCOPE='full'` and `FULL_SCOPE_PASS` for 1984Q1–2026Q2. |
| 2 | The Phase 0 bundle, full-scope Phase 1 run, and its Bronze and reference files. The notebook can acquire FDIC History. To reuse it, retain `data/reference/fdic_history_events.csv` (or a verified Parquet equivalent) with `data/reference/fdic_history_manifest.json`. | The Silver `clean_bank_quarter.parquet` and its `.manifest.json`, plus `bank_quarter_labels_event_based_v3.parquet` and `phase2_to_phase3_handoff_v3.json`. |
| 3 | The matching Phase 2 Silver and V3 labels plus the linked Phase 0/1 lineage and annual Bronze files. | W1 features `w1_financial_features_1985Q1_2026Q2.parquet`, the production manifest, `phase3_to_phase4_handoff_v3.json`, and `outputs/phase3_refined/latest_phase4_handoff.json`. |
| 4 | The Phase 3 pointer, production files, and matched Silver and V3 labels. | The final Top 40 checkpoint and metadata under `outputs/phase4_final40/`. The in-memory Gold path is used when Phase 5 follows in the same kernel. |
| 5 | The Phase 4 checkpoint and same-kernel state. A separate-kernel run also needs a matching `phase4_gold_v3_native.parquet` and `phase4_to_phase5_export.json` from the same release. | For the development sensitivity results, inspect saved §5.3B-COST, STRICT, PIT and CAL outputs (indices 330, 333, 336 and 339). The saved refit starts at §5.3D (index 343). Inspect the archived §5.4A–B output for the frozen scorecard. Run All enters refitting and external scoring. |

The four-quarter Phase 1 pilot (`RUN_SCOPE='pilot'`) is useful for a download smoke test in an isolated checkout. It produces `PILOT_SCOPE_PASS_ONLY`, and Phase 2 requires the full archive. In Phase 3, set `P3_CFG['run_spark'] = True` only when testing Spark parity in a configured Java environment. Phase 2 can analyze assumed publication lags without a separate vintage file. Real publication-time checks need a verified optional file with `CERT`, `REPDTE` and `PUBLIC_AVAILABLE_AT`.

## Replay the reported release or rebuild from FDIC

For a fresh rebuild, start at Phase 0 and let each phase produce its own data, manifests and digests. FDIC can revise source records. If an input digest or coverage gate fails, identify the matching snapshot or regenerate downstream artifacts from the verified source.

To reproduce the reported release without downloading the full history, keep the original `docs/`, `data/` and `outputs/` directories in the same local project. If you update the code, keep the exact original versions of the files pinned by its manifests. A fresh Phase 0 run can produce a different field contract, so run a fresh acquisition in a separate checkout. The first useful checkpoint is the Phase 0/1 bundle, including the same-run Bronze and FDIC reference snapshots. To start at Phase 3 or 4, the archive must also contain the corresponding Silver, V3 and W1 artifacts. A standalone Parquet without its linked manifests does not let the later phases verify lineage.

For remote troubleshooting of a local run, share the first failing cell and its traceback. The Phase 0 artifact manifest and the handoff JSON from the phase immediately before the failure can help identify the required paths and hashes. Large Parquet files can stay on your machine.

The Phase 1 §2.2A audit reads the archived quarterly manifests. It documents completeness at the quarter level and distinguishes archived timestamps from bank-report release vintages. For reproducibility evidence, Phase 5 §5.6A-REPRO and §5.6A-REPRO-ENV document separate checksum, file presence, schema, and dependency checks. The full data replay remains a separate procedure.
