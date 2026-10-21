//                  Topologia da rede de comunicação da Microrrede
//                SMART GRID COMMUNICATION TOPOLOGY (NS-3 + TapBridge)
//                                   
//                                                  access all: 100 Mbps / 0.01 ms                                     
//                               ┌──────▼──────┐       ┌───────────────┐
//                               │     br0     │◄─────►│     EMS       │  
//                               │ (Bridge 0)  │       │   (tap6)      │
//                               └──────┬──────┘       └───────────────┘
//                                      │
//                         rate01 / delay01 (100 Mbps / 2 ms)
//                                      │
//        ┌──────────────┐       ┌──────▼──────┐       ┌──────────────┐
//        │     POI      │◄─────►│     br1     │◄────►│     BESS     │
//        │   (tap0)     │       │ (Bridge 1)  │       │   (tap1)     │
//        └──────────────┘       └──────┬──────┘       └──────────────┘
//                                      │
//                         rate12 / delay12 (50 Mbps / 5 ms)
//                                      │
//        ┌──────────────┐       ┌──────▼──────┐       ┌──────────────┐
//        │     GMG      │◄─────►│     br2     │◄────►│      PV      │
//        │   (tap2)     │       │ (Bridge 2)  │       │   (tap3)     │
//        └──────────────┘       └──────┬──────┘       └──────────────┘
//                                      │
//                         rate23 / delay23 (10 Mbps / 10 ms)
//                                      │
//        ┌──────────────┐       ┌──────▼──────┐       ┌──────────────┐
//        │     LD0      │◄─────►│     br3     │◄────►│     LD1      │
//        │   (tap4)     │       │ (Bridge 3)  │       │   (tap5)     │
//        └──────────────┘       └─────────────┘       └──────────────┘
//
#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/csma-module.h"
#include "ns3/bridge-module.h"
#include "ns3/tap-bridge-module.h"
#include "ns3/netanim-module.h"

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("TapMicrorredeCom4Bridges");

int main(int argc, char* argv[])
{
    uint32_t TSimul = 3600;

    std::string rate01  = "100Mbps";
    std::string delay01 = "2ms";

    std::string rate12  = "50Mbps";
    std::string delay12 = "5ms";

    std::string rate23  = "10Mbps";
    std::string delay23 = "10ms";

    std::string accessRate  = "100Mbps";
    std::string accessDelay = "0.01ms";

    bool enableNetAnim = true;

    CommandLine cmd(__FILE__);
    cmd.AddValue("enableNetAnim", "Habilita NetAnim", enableNetAnim);
    cmd.Parse(argc, argv);

    GlobalValue::Bind("SimulatorImplementationType",
                      StringValue("ns3::RealtimeSimulatorImpl"));
    GlobalValue::Bind("ChecksumEnabled", BooleanValue(true));

    // -----------------------------
    // Nós
    // -----------------------------
    Ptr<Node> br0  = CreateObject<Node>();
    Ptr<Node> br1  = CreateObject<Node>();
    Ptr<Node> br2  = CreateObject<Node>();
    Ptr<Node> br3  = CreateObject<Node>();

    Ptr<Node> ems  = CreateObject<Node>();
    Ptr<Node> poi  = CreateObject<Node>();
    Ptr<Node> bess = CreateObject<Node>();
    Ptr<Node> gmg  = CreateObject<Node>();
    Ptr<Node> pv   = CreateObject<Node>();
    Ptr<Node> ld0  = CreateObject<Node>();
    Ptr<Node> ld1  = CreateObject<Node>();

    // -----------------------------
    // Helpers
    // -----------------------------
    CsmaHelper csmaAccess;
    csmaAccess.SetChannelAttribute("DataRate", StringValue(accessRate));
    csmaAccess.SetChannelAttribute("Delay", StringValue(accessDelay));

    CsmaHelper csma01;
    csma01.SetChannelAttribute("DataRate", StringValue(rate01));
    csma01.SetChannelAttribute("Delay", StringValue(delay01));

    CsmaHelper csma12;
    csma12.SetChannelAttribute("DataRate", StringValue(rate12));
    csma12.SetChannelAttribute("Delay", StringValue(delay12));

    CsmaHelper csma23;
    csma23.SetChannelAttribute("DataRate", StringValue(rate23));
    csma23.SetChannelAttribute("Delay", StringValue(delay23));

    BridgeHelper bridge;
    TapBridgeHelper tapBridge;
    tapBridge.SetAttribute("Mode", StringValue("UseBridge"));

    // -----------------------------
    // BR0
    // -----------------------------
    NetDeviceContainer segBr0Ems = csmaAccess.Install(NodeContainer(br0, ems));
    Ptr<NetDevice> br0_port_ems  = segBr0Ems.Get(0);
    Ptr<NetDevice> ems_dev       = segBr0Ems.Get(1);

    NetDeviceContainer segBr0Br1 = csma01.Install(NodeContainer(br0, br1));
    Ptr<NetDevice> br0_port_br1  = segBr0Br1.Get(0);
    Ptr<NetDevice> br1_port_br0  = segBr0Br1.Get(1);

    NetDeviceContainer br0Ports;
    br0Ports.Add(br0_port_ems);
    br0Ports.Add(br0_port_br1);
    bridge.Install(br0, br0Ports);

    tapBridge.SetAttribute("DeviceName", StringValue("tap6"));
    tapBridge.Install(ems, ems_dev);

    // -----------------------------
    // BR1
    // -----------------------------
    NetDeviceContainer segBr1Poi  = csmaAccess.Install(NodeContainer(br1, poi));
    NetDeviceContainer segBr1Bess = csmaAccess.Install(NodeContainer(br1, bess));

    NetDeviceContainer segBr1Br2 = csma12.Install(NodeContainer(br1, br2));

    NetDeviceContainer br1Ports;
    br1Ports.Add(segBr0Br1.Get(1));
    br1Ports.Add(segBr1Poi.Get(0));
    br1Ports.Add(segBr1Bess.Get(0));
    br1Ports.Add(segBr1Br2.Get(0));
    bridge.Install(br1, br1Ports);

    tapBridge.SetAttribute("DeviceName", StringValue("tap0"));
    tapBridge.Install(poi, segBr1Poi.Get(1));

    tapBridge.SetAttribute("DeviceName", StringValue("tap1"));
    tapBridge.Install(bess, segBr1Bess.Get(1));

    // -----------------------------
    // BR2
    // -----------------------------
    NetDeviceContainer segBr2Gmg = csmaAccess.Install(NodeContainer(br2, gmg));
    NetDeviceContainer segBr2Pv  = csmaAccess.Install(NodeContainer(br2, pv));

    NetDeviceContainer segBr2Br3 = csma23.Install(NodeContainer(br2, br3));

    NetDeviceContainer br2Ports;
    br2Ports.Add(segBr1Br2.Get(1));
    br2Ports.Add(segBr2Gmg.Get(0));
    br2Ports.Add(segBr2Pv.Get(0));
    br2Ports.Add(segBr2Br3.Get(0));
    bridge.Install(br2, br2Ports);

    tapBridge.SetAttribute("DeviceName", StringValue("tap2"));
    tapBridge.Install(gmg, segBr2Gmg.Get(1));

    tapBridge.SetAttribute("DeviceName", StringValue("tap3"));
    tapBridge.Install(pv, segBr2Pv.Get(1));

    // -----------------------------
    // BR3
    // -----------------------------
    NetDeviceContainer segBr3Ld0 = csmaAccess.Install(NodeContainer(br3, ld0));
    NetDeviceContainer segBr3Ld1 = csmaAccess.Install(NodeContainer(br3, ld1));

    NetDeviceContainer br3Ports;
    br3Ports.Add(segBr2Br3.Get(1));
    br3Ports.Add(segBr3Ld0.Get(0));
    br3Ports.Add(segBr3Ld1.Get(0));
    bridge.Install(br3, br3Ports);

    tapBridge.SetAttribute("DeviceName", StringValue("tap4"));
    tapBridge.Install(ld0, segBr3Ld0.Get(1));

    tapBridge.SetAttribute("DeviceName", StringValue("tap5"));
    tapBridge.Install(ld1, segBr3Ld1.Get(1));

    // -----------------------------
    // PCAP
    // -----------------------------
    csmaAccess.EnablePcapAll("comm_ugrid_access", true);
    csma01.EnablePcapAll("comm_ugrid_br0_br1", true);
    csma12.EnablePcapAll("comm_ugrid_br1_br2", true);
    csma23.EnablePcapAll("comm_ugrid_br2_br3", true);

    // -----------------------------
    // NetAnim
    // -----------------------------
    AnimationInterface* anim = nullptr;

    if (enableNetAnim)
    {
        anim = new AnimationInterface("animation.xml");

    // Topologia Simplificada da Rede 
    //   de Comunicação da Microrrede
    //                   EMS(6)
    //                     |
    //                    br0
    //                     |
    //        (0)POI ---  br1   --- BESS(1)
    //                     |
    //        (2)GMG ---  br2   --- PV  (3)
    //                     |
    //        (4)LD0 ---  br3   --- LD1 (5)
    
        // Bridges (coluna central)
     anim->SetConstantPosition(br0,  50.0, 10.0);
     anim->SetConstantPosition(br1,  50.0, 30.0);
     anim->SetConstantPosition(br2,  50.0, 50.0);
     anim->SetConstantPosition(br3,  50.0, 70.0);

     // Dispositivos ligados à br0
     anim->SetConstantPosition(ems,  75.0, 10.0);

     // Dispositivos ligados à br1
     anim->SetConstantPosition(poi,  25.0, 30.0);
     anim->SetConstantPosition(bess, 75.0, 30.0);

     // Dispositivos ligados à br2
     anim->SetConstantPosition(gmg,  25.0, 50.0);
     anim->SetConstantPosition(pv,   75.0, 50.0);

     // Dispositivos ligados à br3
     anim->SetConstantPosition(ld0,  25.0, 70.0);
     anim->SetConstantPosition(ld1,  75.0, 70.0);
    }

    Simulator::Stop(Seconds(TSimul + 1));
    Simulator::Run();

    NS_LOG_UNCOND("Simulação finalizada com sucesso.");

    if (anim != nullptr)
    {
        delete anim;
    }

    Simulator::Destroy();
    return 0;
}
