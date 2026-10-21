# Hybrid microgrid testbed: article sources and supplementary evidence

Supporting material for **A Hybrid OP4610XG and NS-3 Platform for Early Integration Testing of Microgrid EMS and Communication Networks**, prepared during manuscript revision on 21 October 2026. This repository does not assert journal acceptance or an assigned DOI.

The platform connects a physical CompactLogix EMS, an OP4610XG real-time microgrid model and an NS-3 communication network. The supplied measurements are the two nominal protocol acquisitions of **30 April 2026**. No additional experiment was performed to prepare this repository.

## Contents

- **[article_sources/](article_sources/README.txt):** 16 original Go, NS-3, container and network setup files selected from the author-identified `backup_simulador_redes_ugrid` backup. The README explains protocol selection, dependencies and reconstruction limits. Original source bytes are preserved.
- **[nominal_evidence/](nominal_evidence/PROVENANCE.json):** 28 compressed CSV extracts containing 886,087 records from 14 packet captures, 12 device-router logs and two EMS logs.
- **Reanalysis scripts in this directory:** prepared during manuscript revision to reproduce the reported statistics. These are not acquisition programs. They do not start simulations or contact laboratory equipment.
- **[DATA_DICTIONARY.txt](DATA_DICTIONARY.txt)** and **[EVIDENCE_MAP.txt](EVIDENCE_MAP.txt):** field definitions and links between the evidence and manuscript tables.
- **[CONFIGURATION_INVENTORY.json](CONFIGURATION_INVENTORY.json):** recorded settings, source-level declarations and remaining unknowns.
- **[comparative_tasks.csv](comparative_tasks.csv):** six development questions comparing pure NS-3, HIL without network emulation and the hybrid architecture. Alternative platforms were not executed.

## Offline reproduction

Download this repository's ZIP through **Code > Download ZIP**, extract it and open a terminal in its root. Use Python 3; dependency versions are recorded in `requirements.txt`.

```sh
python -m pip install -r requirements.txt
python verify_manifest.py
python reproduce_statistics.py
python reanalyze_available_evidence.py
python verify_reported_results.py
```

Expected checks cover 98 reported numerical values in Tables 8-13 and all 2,704 Modbus operations at four packet boundaries. The 5,408 request/response pairs are observations on two TCP segments, not additional independent operations. [MODBUS_SWEEP_CORRECTION.txt](MODBUS_SWEEP_CORRECTION.txt) documents the corrected sweep boundary in Table 9. Raw acquisition files were not modified.

The extraction program can also be used by a holder of the original April data, as explained in [README.txt](README.txt). Complete PCAP files are not included.

## Scope and provenance

The exported traces omit IP/MAC addresses, absolute timestamps and payload values. The source programs retain laboratory network addresses and configuration paths. The package as a whole is therefore not described as anonymized. Capture clocks and local endpoint labels must not be treated as a shared time base.

The article source backup is separate from the thesis campaign code. Its Modbus main source is byte-identical to the earlier supplied snapshot; its EtherNet/IP source differs only in line endings. The backup adds the NS-3 scenario, device service and deployment context. Source defaults are not an execution-time manifest. In particular, the archived executable metadata and neighboring source files do not establish a consistent source-to-binary-to-acquisition chain. Executables and legacy alternatives are excluded and listed in the source manifest.

Complete controller/LADDER and plant projects, exact NS-3 and acquisition binary provenance, immutable container images and cross-clock calibration are not supplied. No controlled-delay calibration, communication-disturbance/electrical-effect validation, alternative-platform benchmark or measured commissioning-time reduction is claimed.

For a stable reference, use the repository's full Git commit identifier and the file hashes in `FILE_MANIFEST.json`. The material is publicly accessible; no additional reuse license has been selected in this publication step.
