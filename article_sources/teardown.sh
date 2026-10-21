#!/bin/env bash

# --- Configurações ---
CONTAINERS=("interfacePOI" "interfaceBESS" "interfaceGMG" "interfacePV" "interfaceLD0" "interfaceLD1" "interfaceEMS")

# Down the bridges
sudo ip link set bridge0 down
sudo ip link set bridge1 down
sudo ip link set bridge2 down
sudo ip link set bridge3 down
sudo ip link set bridge4 down
sudo ip link set bridge5 down
sudo ip link set bridge6 down
#sudo ip link set br0 down

# Remove the taps from the bridges
sudo ip link set tap0 nomaster
sudo ip link set tap1 nomaster
sudo ip link set tap2 nomaster
sudo ip link set tap3 nomaster
sudo ip link set tap4 nomaster
sudo ip link set tap5 nomaster
sudo ip link set tap6 nomaster

# Delete the bridges
sudo ip link del bridge0
sudo ip link del bridge1
sudo ip link del bridge2
sudo ip link del bridge3
sudo ip link del bridge4
sudo ip link del bridge5
sudo ip link del bridge6
#sudo ip link del br0

# Delete the taps
sudo ip link del tap0
sudo ip link del tap1
sudo ip link del tap2
sudo ip link del tap3
sudo ip link del tap4
sudo ip link del tap5
sudo ip link del tap6

sudo ip link del veth0-b
sudo ip link del veth1-b
sudo ip link del veth2-b
sudo ip link del veth3-b
sudo ip link del veth4-b
sudo ip link del veth5-b
sudo ip link del veth6-b

#Salvar os arquivos de Captura de pacotes .pcap
sudo docker cp "${CONTAINERS[0]}":/app/capture.pcap ./Dados/capture_interfaces/capturePOI.pcap 
sudo docker cp "${CONTAINERS[1]}":/app/capture.pcap ./Dados/capture_interfaces/captureBESS.pcap  
sudo docker cp "${CONTAINERS[2]}":/app/capture.pcap ./Dados/capture_interfaces/captureGMG.pcap  
sudo docker cp "${CONTAINERS[3]}":/app/capture.pcap ./Dados/capture_interfaces/capturePV.pcap  
sudo docker cp "${CONTAINERS[4]}":/app/capture.pcap ./Dados/capture_interfaces/captureLD0.pcap  
sudo docker cp "${CONTAINERS[5]}":/app/capture.pcap ./Dados/capture_interfaces/captureLD1.pcap
sudo docker cp "${CONTAINERS[6]}":/app/capture.pcap ./Dados/capture_interfaces/captureEMS.pcap  

# Salvar arquivos de performace .csv 
sudo docker cp "${CONTAINERS[0]}":/app/device_router_metrics.csv ./Dados/capture_interfaces/device_router_metricsPOI.csv
sudo docker cp "${CONTAINERS[1]}":/app/device_router_metrics.csv ./Dados/capture_interfaces/device_router_metricsBESS.csv
sudo docker cp "${CONTAINERS[2]}":/app/device_router_metrics.csv ./Dados/capture_interfaces/device_router_metricsGMG.csv
sudo docker cp "${CONTAINERS[3]}":/app/device_router_metrics.csv ./Dados/capture_interfaces/device_router_metricsPV.csv
sudo docker cp "${CONTAINERS[4]}":/app/device_router_metrics.csv ./Dados/capture_interfaces/device_router_metricsLD0.csv
sudo docker cp "${CONTAINERS[5]}":/app/device_router_metrics.csv ./Dados/capture_interfaces/device_router_metricsLD1.csv
sudo docker cp "${CONTAINERS[6]}":/app/plc_scan_metrics.csv ./Dados/capture_interfaces/plc_scan_metrics.csv
sudo docker cp "${CONTAINERS[6]}":/app/device_plc.csv ./Dados/capture_interfaces/device_plc.csv 


# Stop all running container
sudo docker compose -f tap-csma-cenario.yaml down

# Verify if still any container existed
sudo docker container ls -a

#cd ./ns-3-dev

# Zipar para analise dos dados de captura
zip /home/alyston/workspace/Dados/metrics-last.zip /home/alyston/workspace/Dados/capture_interfaces/*.pcap

#cd ..

# Zipar para analise dos dados do gateways do Opal
zip /home/alyston/workspace/Dados/device_router_metrics.zip /home/alyston/workspace/Dados/capture_interfaces/device_router_metrics*.csv


# Rodar as analises:

