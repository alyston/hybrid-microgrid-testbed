SUPPLEMENTARY MATERIAL - Access-2026-41440
Scope: two protocol acquisitions on 30 April 2026. No new experiment was performed.

Run from this directory, using Python 3 (tested runtime in expanded_reanalysis_results.json):
  python -m pip install -r requirements.txt
  python verify_manifest.py
  python reproduce_statistics.py
  python reanalyze_available_evidence.py
  python verify_reported_results.py

The first script checks distributed files. The second reproduces the original count/scan audit. The third reconstructs the nominal transaction, scan, router and capture statistics used in Tables 8-13. The fourth checks those results against the manuscript's reported precision and verifies the original Modbus correspondence. Expected outputs are recomputed_results.json, expanded_reanalysis_results.json and verification_report.json. None of these commands contacts a controller or starts a simulation.

New inputs: nominal_evidence/ contains 28 compressed CSV files: metadata from 14 packet captures, all retained rows from 12 device-router logs and both EMS application logs. EXTRACT_INVENTORY.json gives exact row counts. PROVENANCE.json records original filenames/hashes, extract hashes and exclusions. DATA_DICTIONARY.txt defines fields and time boundaries. CONFIGURATION_INVENTORY.json separates recorded settings, compatible archived source and unavailable deployment details.

For a holder of the original Dados directory, the extraction can be repeated separately:
  python extract_nominal_evidence.py --data-root /path/to/Dados --output /path/to/new_export
This reads only the named April files. Gzip container bytes can differ between runs; decoded rows and their order are the reproducibility target. The supplied hashes identify the shipped files.

Article sources: article_sources/ supplies 16 byte-preserved source, dependency and deployment files from the backup identified by the author. It includes both EMS protocol variants, the device interface, the NS-3 topology, Compose/Dockerfiles and network setup/teardown scripts. SOURCE_MANIFEST.json and RECOVERED_CONFIGURATION.json distinguish source defaults from execution provenance. These are separate from the revision reanalysis scripts.

Existing inputs: modbus_matched_operations.csv and modbus_audit_summary.json; eip_nominal_deidentified.csv and eip_nominal_audit.json; two archived EMS source snapshots and their manifest. Those six evidence/source files are unchanged. The archived Go programs are compatible snapshots, not proof of the exact executable used.

Comparison: comparative_tasks.csv and COMPARATIVE_EVALUATION.txt apply common development questions to pure NS-3, HIL without network emulation and the present hybrid architecture. Existing data support only the hybrid column. Alternatives are evaluated analytically from their defined architecture and cited documentation; they were not executed. There is no numerical speed, cost or commissioning-time ranking.

MODBUS_SWEEP_CORRECTION.txt documents a corrected reconstruction boundary in Table 9. It changes three derived rows, not the acquisition, operation counts or POI-to-POI cycle results.

Privacy and limits: CSV exports omit IP/MAC addresses, absolute dates and payload values. Host labels are local to each capture; independent capture clocks must not be combined. Payload hashes support identity comparisons but do not disclose their values. Original source snapshots retain laboratory addresses; the complete archive is therefore not described as address-free. Full PCAPs, electrical plant and controller projects, exact nominal NS-3/Go builds and clock calibration are not supplied. The recovered network/deployment sources document defaults and real-time scheduler selection, but do not establish the actual execution state or scheduler accuracy. The exports support statistical reanalysis, not exact reconstruction of the deployment. No disturbance or electrical-effect validation is claimed. See EVIDENCE_MAP.txt for exclusions and scope.

Public repository: https://github.com/alyston/hybrid-microgrid-testbed
The repository version is identified by its Git commit. No DOI is assigned to this supplement.
