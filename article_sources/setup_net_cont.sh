#!/bin/env bash

# Exit immediately if a commands exits with non-zero status
set -e

# --- Configurações ---
CONTAINERS=("interfacePOI" "interfaceBESS" "interfaceGMG" "interfacePV" "interfaceLD0" "interfaceLD1" "interfaceEMS")

SCRIPT1_DIR="Image_Dev/"
SCRIPT1_NOME="criar_img_dev.sh"

SCRIPT2_DIR="Image_EMS/"
SCRIPT2_NOME="criar_img_ems.sh"

# --- Inicio da Criação das imagens Device e EMS ---
echo "--- Inicio da Criação das imagens ---"
cd $SCRIPT1_DIR 
bash "$SCRIPT1_NOME"
cd ..
cd $SCRIPT2_DIR 
bash "$SCRIPT2_NOME"
cd ..
echo "--- Final da Criação das imagens ---"

# Add bridges
echo "Adicionar bridges..."
sudo ip link add name bridge0 type bridge
sudo ip link add name bridge1 type bridge
sudo ip link add name bridge2 type bridge
sudo ip link add name bridge3 type bridge
sudo ip link add name bridge4 type bridge
sudo ip link add name bridge5 type bridge
sudo ip link add name bridge6 type bridge
echo "Concluido."

# Add tap devices
echo "Adicionar tap devices..."
sudo ip tuntap add tap0 mode tap
sudo ip tuntap add tap1 mode tap
sudo ip tuntap add tap2 mode tap
sudo ip tuntap add tap3 mode tap
sudo ip tuntap add tap4 mode tap
sudo ip tuntap add tap5 mode tap
sudo ip tuntap add tap6 mode tap
echo "Concluido."

echo "Configurar tap no modo promiscuo..."
sudo ip link set tap0 promisc on
sudo ip link set tap1 promisc on
sudo ip link set tap2 promisc on
sudo ip link set tap3 promisc on
sudo ip link set tap4 promisc on
sudo ip link set tap5 promisc on
sudo ip link set tap6 promisc on
echo "Concluido."

# Attach tap devices to bridges and activate
echo "Conectar taps as bridges..."
sudo ip link set tap0 master bridge0
sudo ip link set bridge0 up
sudo ip link set tap1 master bridge1
sudo ip link set bridge1 up
sudo ip link set tap2 master bridge2
sudo ip link set bridge2 up
sudo ip link set tap3 master bridge3
sudo ip link set bridge3 up
sudo ip link set tap4 master bridge4
sudo ip link set bridge4 up
sudo ip link set tap5 master bridge5
sudo ip link set bridge5 up
sudo ip link set tap6 master bridge6
sudo ip link set bridge6 up

sudo ip link set tap0 up
sudo ip link set tap1 up
sudo ip link set tap2 up
sudo ip link set tap3 up
sudo ip link set tap4 up
sudo ip link set tap5 up
sudo ip link set tap6 up
echo "Concluido."

# disallow bridge traffic to go through ip tables chain
# See: https://unix.stackexchange.com/questions/499756/how-does-iptable-work-with-linux-bridge
# and: https://wiki.libvirt.org/page/Net.bridge.bridge-nf-call_and_sysctl.conf
# parte original
#pushd /proc/sys/net/bridge
#for f in bridge-nf-*; do echo 0 > $f; done
#popd

# programa adaptado para rodar com sudo
pushd /proc/sys/net/bridge > /dev/null
for f in bridge-nf-*; do
    echo 0 | sudo tee "$f" > /dev/null
done
popd > /dev/null



# Create the network namespace runtime folder if not exists
sudo mkdir -p /var/run/netns

export pid0=0
export pid1=0
export pid2=0
export pid3=0
export pid4=0
export pid5=0
export pid6=0

# Run container orchestrator
echo "Iniciar containers..."
sudo docker compose -f tap-csma-cenario.yaml up -d
echo "Containers Iniciados."
pid0=$(sudo docker inspect --format '{{ .State.Pid }}' "${CONTAINERS[0]}")
pid1=$(sudo docker inspect --format '{{ .State.Pid }}' "${CONTAINERS[1]}")
pid2=$(sudo docker inspect --format '{{ .State.Pid }}' "${CONTAINERS[2]}")
pid3=$(sudo docker inspect --format '{{ .State.Pid }}' "${CONTAINERS[3]}")
pid4=$(sudo docker inspect --format '{{ .State.Pid }}' "${CONTAINERS[4]}")
pid5=$(sudo docker inspect --format '{{ .State.Pid }}' "${CONTAINERS[5]}")
pid6=$(sudo docker inspect --format '{{ .State.Pid }}' "${CONTAINERS[6]}")


# Soft-link the network namespace created by container into the linux namespace runtime
sudo ln -s /proc/$pid0/ns/net /var/run/netns/$pid0
sudo ln -s /proc/$pid1/ns/net /var/run/netns/$pid1
sudo ln -s /proc/$pid2/ns/net /var/run/netns/$pid2
sudo ln -s /proc/$pid3/ns/net /var/run/netns/$pid3
sudo ln -s /proc/$pid4/ns/net /var/run/netns/$pid4
sudo ln -s /proc/$pid5/ns/net /var/run/netns/$pid5
sudo ln -s /proc/$pid6/ns/net /var/run/netns/$pid6


# Create Veth pair to attach to bridge
echo "Create Veth pairs..."
sudo ip link add veth0-b type veth peer name veth0-c
sudo ip link set veth0-b master bridge0
sudo ip link set veth0-b up

sudo ip link add veth1-b type veth peer name veth1-c
sudo ip link set veth1-b master bridge1
sudo ip link set veth1-b up

sudo ip link add veth2-b type veth peer name veth2-c
sudo ip link set veth2-b master bridge2
sudo ip link set veth2-b up

sudo ip link add veth3-b type veth peer name veth3-c
sudo ip link set veth3-b master bridge3
sudo ip link set veth3-b up

sudo ip link add veth4-b type veth peer name veth4-c
sudo ip link set veth4-b master bridge4
sudo ip link set veth4-b up

sudo ip link add veth5-b type veth peer name veth5-c
sudo ip link set veth5-b master bridge5
sudo ip link set veth5-b up

sudo ip link add veth6-b type veth peer name veth6-c
sudo ip link set veth6-b master bridge6
sudo ip link set veth6-b up

# Configure the container-side pair with an interface and address
sudo ip link set veth0-c netns $pid0
sudo ip netns exec $pid0 ip link set dev veth0-c name eth1
sudo ip netns exec $pid0 ip link set eth1 address 12:34:88:5D:61:BA
sudo ip netns exec $pid0 ip link set eth1 up
sudo ip netns exec $pid0 ip addr add 192.168.1.30/24 dev eth1

sudo ip link set veth1-c netns $pid1
sudo ip netns exec $pid1 ip link set dev veth1-c name eth1
sudo ip netns exec $pid1 ip link set eth1 address 12:34:88:5D:61:BB
sudo ip netns exec $pid1 ip link set eth1 up
sudo ip netns exec $pid1 ip addr add 192.168.1.31/24 dev eth1

sudo ip link set veth2-c netns $pid2
sudo ip netns exec $pid2 ip link set dev veth2-c name eth1
sudo ip netns exec $pid2 ip link set eth1 address 12:34:88:5D:61:BC
sudo ip netns exec $pid2 ip link set eth1 up
sudo ip netns exec $pid2 ip addr add 192.168.1.32/24 dev eth1

sudo ip link set veth3-c netns $pid3
sudo ip netns exec $pid3 ip link set dev veth3-c name eth1
sudo ip netns exec $pid3 ip link set eth1 address 12:34:88:5D:61:BD
sudo ip netns exec $pid3 ip link set eth1 up
sudo ip netns exec $pid3 ip addr add 192.168.1.33/24 dev eth1

sudo ip link set veth4-c netns $pid4
sudo ip netns exec $pid4 ip link set dev veth4-c name eth1
sudo ip netns exec $pid4 ip link set eth1 address 12:34:88:5D:61:BE
sudo ip netns exec $pid4 ip link set eth1 up
sudo ip netns exec $pid4 ip addr add 192.168.1.34/24 dev eth1

sudo ip link set veth5-c netns $pid5
sudo ip netns exec $pid5 ip link set dev veth5-c name eth1
sudo ip netns exec $pid5 ip link set eth1 address 12:34:88:5D:61:BF
sudo ip netns exec $pid5 ip link set eth1 up
sudo ip netns exec $pid5 ip addr add 192.168.1.35/24 dev eth1

sudo ip link set veth6-c netns $pid6
sudo ip netns exec $pid6 ip link set dev veth6-c name eth1
sudo ip netns exec $pid6 ip link set eth1 address 12:34:88:5D:61:C0
sudo ip netns exec $pid6 ip link set eth1 up
sudo ip netns exec $pid6 ip addr add 192.168.1.10/24 dev eth1

for NOME_CONTAINER in "${CONTAINERS[@]}"; do
    echo "Configurando modo promíscuo em: $NOME_CONTAINER"
    sudo docker exec -it "$NOME_CONTAINER" ip link set eth1 promisc on
done

echo "Done."
echo "### Setup completo. Pronto para Iniciar a Simulacao ###"

echo "Executar as interfaces"

sudo docker exec -d "${CONTAINERS[0]}" ./device_router 
sudo docker exec -d "${CONTAINERS[1]}" ./device_router 
sudo docker exec -d "${CONTAINERS[2]}" ./device_router 
sudo docker exec -d "${CONTAINERS[3]}" ./device_router 
sudo docker exec -d "${CONTAINERS[4]}" ./device_router 
sudo docker exec -d "${CONTAINERS[5]}" ./device_router   
sudo docker exec -d "${CONTAINERS[6]}" ./ems_router_cplx

echo " ##### Done ##### "

# Executar o tcpdump para captura dos pacotes
echo "Executar o tcpdump"
sudo docker exec -d "${CONTAINERS[0]}" tcpdump -i any -w /app/capture.pcap
sudo docker exec -d "${CONTAINERS[1]}" tcpdump -i any -w /app/capture.pcap
sudo docker exec -d "${CONTAINERS[2]}" tcpdump -i any -w /app/capture.pcap
sudo docker exec -d "${CONTAINERS[3]}" tcpdump -i any -w /app/capture.pcap
sudo docker exec -d "${CONTAINERS[4]}" tcpdump -i any -w /app/capture.pcap
sudo docker exec -d "${CONTAINERS[5]}" tcpdump -i any -w /app/capture.pcap 
sudo docker exec -d "${CONTAINERS[6]}" tcpdump -i any -w /app/capture.pcap
echo " ##### Done ##### "

echo "Inicializar a rede Virtual NS-3"
gnome-terminal -- bash -c "cd ns-3-dev;./ns3 run scratch/comun_ugrid; exec bash"
echo " ##### Rede Simulada Rodando ##### "
