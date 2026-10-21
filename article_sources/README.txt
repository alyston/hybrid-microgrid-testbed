ARTICLE SOURCE BACKUP - supplied by the author on 21 October 2026

This directory contains 16 selected Go/NS-3/container/network source and dependency files from backup_simulador_redes_ugrid. They are distinct from the statistical reanalysis scripts in the parent directory, which were prepared during manuscript revision. Original source bytes and relative paths are preserved. SOURCE_MANIFEST.json identifies the copies and the files excluded from the received backup. The thesis campaign code is not used here.

Entry points:
  comun_ugrid.cc: seven external endpoints connected through four simulated bridges.
  Image_Dev/device_router.go: the common OPAL-RT/device interface.
  Image_EMS/ems_router_cplx.go: the selected root source is the Modbus/TCP router.
  Image_EMS/Arquivos_go/ems_router_cplx_eip.go: EtherNet/IP alternative.
  Image_EMS/Arquivos_go/ems_router_cplx_MB.go: Modbus/TCP alternative.
  setup_net_cont.sh / teardown.sh: host/container network setup and collection/teardown.
  tap-csma-cenario.yaml: six device containers plus one EMS container.

The root Modbus source is byte-identical to the earlier supplied snapshot. The EtherNet/IP source is text-identical to its earlier snapshot after normalizing line endings. Thus identification of the backup adds origin and deployment context without changing the measured results. These files are author-identified article sources; there is no acquisition-time build manifest proving the exact executable used.

RECOVERED_CONFIGURATION.json records source-level settings. The NS-3 source explicitly binds RealtimeSimulatorImpl, enables checksums and uses TapBridge UseBridge. Its link-delay defaults are configuration values, not a controlled-delay validation experiment. The exact NS-3 release, synchronization mode and scheduler-lag accuracy were not recovered. The Go module language directive is 1.22.2; Dockerfiles select golang:1.24-alpine and alpine:latest without immutable image digests. These are different evidence types and do not identify the acquisition compiler.

Protocol selection for reconstruction in a COPY of the backup:
  The provided Image_EMS/ems_router_cplx.go builds the Modbus router.
  To select EtherNet/IP, copy Image_EMS/Arquivos_go/ems_router_cplx_eip.go over Image_EMS/ems_router_cplx.go in that working copy before rebuilding the EMS image.
  The two variant Go programs have their own main functions and must not be compiled together as one package.

Deployment prerequisites and adaptation points:
  A dedicated Linux laboratory host, Docker/Compose, iproute2 and an NS-3 build with CSMA, bridge, TapBridge, NetAnim and real-time support are needed. The setup script expects ns-3-dev/scratch/comun_ugrid.cc relative to its working directory. NS-3 itself is not bundled. The installed laboratory must supply the OPAL-RT plant and CompactLogix application, tags and AOI/LADDER logic.
  Network names, interface eno1, private IP/MAC addresses and output paths are laboratory-specific. Review them before deploying. The Compose file retains build: . while images are built separately in Image_Dev and Image_EMS; the backup has no root Dockerfile. This requires checking the intended image/build selection on the target installation. The setup starts compiled interfaces and captures; it changes network namespaces, bridges, TAPs and host bridge-filter settings. The teardown stops the named containers and deletes their named network interfaces. Neither script was run during this revision.
  Compiled executables, older Python implementations and antigos directories are excluded. The archived binaries' metadata does not consistently match the neighboring current sources, so the executables are not presented as acquisition artifacts. Hashes of excluded backup files remain in SOURCE_MANIFEST.json for provenance.

No new experiment, deployed reconstruction, compiler validation, timing calibration or electrical-response validation is claimed. The statistical reanalysis in the parent directory runs offline and does not require the laboratory deployment.
