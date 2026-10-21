package main

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/danomagnum/gologix"
)

const (
	virtualInterface = "eth1"
	plcIP            = "192.168.0.10"
	modbusPort       = 502

	plcCycle       = 150 * time.Millisecond
	plcReadLatency = 50 * time.Millisecond
	scanPeriod     = plcCycle + plcReadLatency
	virtualCycle   = 50 * time.Millisecond
	staleTimeout   = 1 * time.Second

	modbusConnectTimeout = 2 * time.Second
	modbusReadTimeout    = 2 * time.Second
	modbusWriteTimeout   = 2 * time.Second
	modbusAddrStart      = 0
	modbusQtyAll         = 44

	reconnectInterval = 2 * time.Second

	enablePLCInteractionCSV    = true
	plcInteractionCSVPath      = "./plc_scan_metrics.csv"
	enableDeviceInteractionCSV = true
	deviceInteractionCSVPath   = "./device_plc.csv"
)

type DeviceConfig struct {
	Name        string
	UnitID      uint8
	VirtualIP   string
	StatusTag   string
	ComTag      string
	FeedbackTag string
	RefTag      string
}

type Payload struct {
	Status   [2]int32
	Feedback [20]float32
}

type CommandPayload struct {
	Com [2]int32
	Ref [20]float32
}

type ScanMetrics struct {
	Timestamp      time.Time
	ScanIndex      uint64
	WritePhase     time.Duration
	LatencyPhase   time.Duration
	ReadPhase      time.Duration
	TotalPhase     time.Duration
	Budget         time.Duration
	Overrun        bool
	OverrunBy      time.Duration
	DevicesWritten int
	DevicesRead    int
}

var devices = []DeviceConfig{
	{"POI", 0, "192.168.1.30", "POI_Status", "POI_Com", "POI_Feedback", "POI_Ref"},
	{"BESS", 1, "192.168.1.31", "BESS_Status", "BESS_Com", "BESS_Feedback", "BESS_Ref"},
	{"GMG", 2, "192.168.1.32", "GMG_Status", "GMG_Com", "GMG_Feedback", "GMG_Ref"},
	{"PV", 3, "192.168.1.33", "PV_Status", "PV_Com", "PV_Feedback", "PV_Ref"},
	{"LD0", 4, "192.168.1.34", "LD0_Status", "LD0_Com", "LD0_Feedback", "LD0_Ref"},
	{"LD1", 5, "192.168.1.35", "LD1_Status", "LD1_Com", "LD1_Feedback", "LD1_Ref"},
}

type RxSlot struct {
	Payload    Payload
	LastUpdate time.Time
	Version    uint64
	LastSource string
}

type TxSlot struct {
	Payload         CommandPayload
	LastUpdate      time.Time
	Version         uint64
	LastSentVersion uint64
	HasNewData      bool
}

type DeviceMemory struct {
	mu sync.RWMutex
	Rx RxSlot
	Tx TxSlot
}

type SharedMemory struct {
	Devices map[string]*DeviceMemory
}

func NewSharedMemory(devs []DeviceConfig) *SharedMemory {
	m := &SharedMemory{Devices: make(map[string]*DeviceMemory)}
	for _, d := range devs {
		m.Devices[d.Name] = &DeviceMemory{}
	}
	return m
}

func (m *SharedMemory) UpdateRx(devName string, p Payload, src string) {
	d := m.Devices[devName]
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Rx.Payload = p
	d.Rx.LastUpdate = time.Now()
	d.Rx.Version++
	d.Rx.LastSource = src
}

func (m *SharedMemory) ReadRx(devName string) (Payload, time.Time, uint64, string) {
	d := m.Devices[devName]
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.Rx.Payload, d.Rx.LastUpdate, d.Rx.Version, d.Rx.LastSource
}

func (m *SharedMemory) UpdateTx(devName string, p CommandPayload) bool {
	d := m.Devices[devName]
	d.mu.Lock()
	defer d.mu.Unlock()
	if reflect.DeepEqual(d.Tx.Payload, p) {
		return false
	}
	d.Tx.Payload = p
	d.Tx.LastUpdate = time.Now()
	d.Tx.Version++
	d.Tx.HasNewData = true
	return true
}

func (m *SharedMemory) ReadTx(devName string) (CommandPayload, time.Time, uint64, bool, uint64) {
	d := m.Devices[devName]
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.Tx.Payload, d.Tx.LastUpdate, d.Tx.Version, d.Tx.HasNewData, d.Tx.LastSentVersion
}

func (m *SharedMemory) MarkTxSent(devName string, version uint64) {
	d := m.Devices[devName]
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Tx.LastSentVersion = version
	if d.Tx.Version == version {
		d.Tx.HasNewData = false
	}
}

type InteractionRecord struct {
	Timestamp    time.Time
	LoopType     string
	LoopIndex    uint64
	Device       string
	Protocol     string
	FunctionCode string
	Operation    string
	Direction    string
	Target       string
	Status       string
	Duration     time.Duration
	Details      string
	Error        string
	ScanWrite    time.Duration
	ScanLatency  time.Duration
	ScanRead     time.Duration
	ScanTotal    time.Duration
	Budget       time.Duration
	Overrun      bool
	OverrunBy    time.Duration
}

type InteractionCSVWriter struct {
	enabled bool
	path    string
	mu      sync.Mutex
	file    *os.File
	writer  *csv.Writer
}

func NewInteractionCSVWriter(enabled bool, path string) (*InteractionCSVWriter, error) {
	w := &InteractionCSVWriter{enabled: enabled, path: path}
	if !enabled {
		return w, nil
	}
	fileExists := true
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fileExists = false
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	w.file = f
	w.writer = csv.NewWriter(f)
	if !fileExists {
		if err := w.writer.Write([]string{
			"timestamp", "loop_type", "loop_index", "device", "protocol", "function_code",
			"operation", "direction", "target", "status", "duration_ms", "details", "error",
			"scan_write_ms", "scan_latency_ms", "scan_read_ms", "scan_total_ms", "budget_ms", "overrun", "overrun_by_ms",
		}); err != nil {
			_ = f.Close()
			return nil, err
		}
		w.writer.Flush()
		if err := w.writer.Error(); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return w, nil
}

func (w *InteractionCSVWriter) Write(r InteractionRecord) error {
	if !w.enabled {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	record := []string{
		r.Timestamp.Format(time.RFC3339Nano),
		r.LoopType,
		fmt.Sprintf("%d", r.LoopIndex),
		r.Device,
		r.Protocol,
		r.FunctionCode,
		r.Operation,
		r.Direction,
		r.Target,
		r.Status,
		fmt.Sprintf("%.6f", float64(r.Duration.Microseconds())/1000.0),
		r.Details,
		r.Error,
		fmt.Sprintf("%.6f", float64(r.ScanWrite.Microseconds())/1000.0),
		fmt.Sprintf("%.6f", float64(r.ScanLatency.Microseconds())/1000.0),
		fmt.Sprintf("%.6f", float64(r.ScanRead.Microseconds())/1000.0),
		fmt.Sprintf("%.6f", float64(r.ScanTotal.Microseconds())/1000.0),
		fmt.Sprintf("%.6f", float64(r.Budget.Microseconds())/1000.0),
		strconv.FormatBool(r.Overrun),
		fmt.Sprintf("%.6f", float64(r.OverrunBy.Microseconds())/1000.0),
	}
	if err := w.writer.Write(record); err != nil {
		return err
	}
	w.writer.Flush()
	return w.writer.Error()
}

func (w *InteractionCSVWriter) Close() error {
	if !w.enabled {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writer != nil {
		w.writer.Flush()
		if err := w.writer.Error(); err != nil {
			if w.file != nil {
				_ = w.file.Close()
			}
			return err
		}
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	virtualIP, err := getInterfaceIPv4(virtualInterface)
	if err != nil {
		log.Fatalf("erro ao obter IP de %s: %v", virtualInterface, err)
	}

	plcWriter, err := NewInteractionCSVWriter(enablePLCInteractionCSV, plcInteractionCSVPath)
	if err != nil {
		log.Fatalf("erro abrindo writer de interações PLC: %v", err)
	}
	defer func() {
		if err := plcWriter.Close(); err != nil {
			log.Printf("erro fechando arquivo de interações PLC: %v", err)
		}
	}()

	deviceWriter, err := NewInteractionCSVWriter(enableDeviceInteractionCSV, deviceInteractionCSVPath)
	if err != nil {
		log.Fatalf("erro abrindo writer de interações Modbus: %v", err)
	}
	defer func() {
		if err := deviceWriter.Close(); err != nil {
			log.Printf("erro fechando arquivo de interações Modbus: %v", err)
		}
	}()

	log.Printf(
		"interfaceEMS EIP<->Modbus iniciado | %s=%s | PLC(EtherNet/IP)=%s | Modbus/TCP=%d | plcCycle=%s | readLatency=%s | scanPeriod=%s | virtualCycle=%s | stale=%s | plcCSV=%t | plcCSVPath=%s | deviceCSV=%t | deviceCSVPath=%s",
		virtualInterface, virtualIP, plcIP, modbusPort,
		plcCycle.String(), plcReadLatency.String(), scanPeriod.String(), virtualCycle.String(), staleTimeout.String(),
		enablePLCInteractionCSV, plcInteractionCSVPath, enableDeviceInteractionCSV, deviceInteractionCSVPath,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	plc := NewPLCBridge(plcIP)
	if err := plc.Connect(); err != nil {
		log.Printf("PLC indisponível no início: %v", err)
		log.Printf("o programa continuará tentando reconectar automaticamente")
	}
	defer plc.Close()

	mem := NewSharedMemory(devices)
	var wg sync.WaitGroup
	errCh := make(chan error, len(devices)+1)

	for _, dev := range devices {
		dev := dev
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runVirtualModbusLoop(ctx, dev, mem, virtualCycle, deviceWriter); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("[%s][VIRTUAL-MODBUS] %w", dev.Name, err)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runPLCScan(ctx, devices, plc, mem, staleTimeout, plcReadLatency, scanPeriod, plcWriter); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("[PLC-SCAN] %w", err)
		}
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		log.Printf("encerrando interfaceEMS EIP<->Modbus")
	case err := <-errCh:
		if err != nil {
			log.Printf("erro: %v", err)
			stop()
		}
	}

	wg.Wait()
}

type PLCBridge struct {
	ip     string
	client *gologix.Client
	mu     sync.Mutex
}

func NewPLCBridge(ip string) *PLCBridge { return &PLCBridge{ip: ip} }

func statusToSlice(a [2]int32) []int32 { return []int32{a[0], a[1]} }

func feedbackToSlice(a [20]float32) []float32 {
	out := make([]float32, 20)
	copy(out, a[:])
	return out
}

func anySliceToCom(v any) ([2]int32, error) {
	var out [2]int32
	items, ok := v.([]interface{})
	if !ok {
		return out, fmt.Errorf("Com não retornou []interface{}, retornou %T", v)
	}
	if len(items) != 2 {
		return out, fmt.Errorf("Com retornou %d elementos, esperado 2", len(items))
	}
	for i := 0; i < 2; i++ {
		switch x := items[i].(type) {
		case int32:
			out[i] = x
		case uint32:
			out[i] = int32(x)
		case int:
			out[i] = int32(x)
		case uint:
			out[i] = int32(x)
		case int16:
			out[i] = int32(x)
		case uint16:
			out[i] = int32(x)
		case int64:
			out[i] = int32(x)
		case uint64:
			out[i] = int32(x)
		case float32:
			out[i] = int32(x)
		case float64:
			out[i] = int32(x)
		default:
			return out, fmt.Errorf("Com[%d] tipo inesperado: %T", i, items[i])
		}
	}
	return out, nil
}

func anySliceToRef(v any) ([20]float32, error) {
	var out [20]float32
	items, ok := v.([]interface{})
	if !ok {
		return out, fmt.Errorf("Ref não retornou []interface{}, retornou %T", v)
	}
	if len(items) != 20 {
		return out, fmt.Errorf("Ref retornou %d elementos, esperado 20", len(items))
	}
	for i := 0; i < 20; i++ {
		switch x := items[i].(type) {
		case float32:
			out[i] = x
		case float64:
			out[i] = float32(x)
		case int32:
			out[i] = float32(x)
		case int:
			out[i] = float32(x)
		case int64:
			out[i] = float32(x)
		default:
			return out, fmt.Errorf("Ref[%d] tipo inesperado: %T", i, items[i])
		}
	}
	return out, nil
}

func (p *PLCBridge) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.connectLocked()
}

func (p *PLCBridge) connectLocked() error {
	if p.client != nil {
		p.client.Disconnect()
		p.client = nil
	}
	c := gologix.NewClient(p.ip)
	path, err := gologix.ParsePath("1,0")
	if err != nil {
		return fmt.Errorf("erro ao montar path CIP 1,0: %w", err)
	}
	c.Controller.Path = path
	c.Controller.Port = 44818
	c.SocketTimeout = 5 * time.Second
	c.AutoConnect = true

	log.Printf("tentando conectar ao PLC %s com path 1,0", p.ip)
	if err := c.Connect(); err != nil {
		return fmt.Errorf("erro no Connect: %w", err)
	}
	p.client = c
	log.Printf("conectado ao PLC %s com path 1,0", p.ip)
	return nil
}

func (p *PLCBridge) ensureConnectedLocked() error {
	if p.client != nil {
		return nil
	}
	return p.connectLocked()
}

func (p *PLCBridge) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		p.client.Disconnect()
		p.client = nil
	}
}

func (p *PLCBridge) markDisconnectedLocked() {
	if p.client != nil {
		p.client.Disconnect()
		p.client = nil
	}
}

func (p *PLCBridge) reconnectWithRetry(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		p.mu.Lock()
		err := p.connectLocked()
		p.mu.Unlock()
		if err == nil {
			return nil
		}
		log.Printf("falha ao reconectar ao PLC %s: %v", p.ip, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnectInterval):
		}
	}
}

func (p *PLCBridge) WriteStatusFeedback(ctx context.Context, dev DeviceConfig, payload Payload) error {
	for {
		p.mu.Lock()
		if err := p.ensureConnectedLocked(); err != nil {
			p.mu.Unlock()
			if err := p.reconnectWithRetry(ctx); err != nil {
				return err
			}
			continue
		}
		status := statusToSlice(payload.Status)
		feedback := feedbackToSlice(payload.Feedback)
		err1 := p.client.Write(dev.StatusTag+"[0]", status)
		err2 := p.client.Write(dev.FeedbackTag+"[0]", feedback)
		if err1 == nil && err2 == nil {
			p.mu.Unlock()
			return nil
		}
		log.Printf("[%s] erro escrevendo PLC em bloco (%v | %v), reconectando...", dev.Name, err1, err2)
		p.markDisconnectedLocked()
		p.mu.Unlock()
		if err := p.reconnectWithRetry(ctx); err != nil {
			return err
		}
	}
}

func (p *PLCBridge) ReadComRef(ctx context.Context, dev DeviceConfig) (CommandPayload, error) {
	var out CommandPayload
	for {
		p.mu.Lock()
		if err := p.ensureConnectedLocked(); err != nil {
			p.mu.Unlock()
			if err := p.reconnectWithRetry(ctx); err != nil {
				return out, err
			}
			continue
		}
		readMap := map[string]any{
			dev.ComTag + "[0]": make([]int32, 2),
			dev.RefTag + "[0]": make([]float32, 20),
		}
		err := p.client.ReadMap(readMap)
		if err == nil {
			p.mu.Unlock()
			com, err := anySliceToCom(readMap[dev.ComTag+"[0]"])
			if err != nil {
				return out, fmt.Errorf("erro convertendo %s: %w", dev.ComTag, err)
			}
			ref, err := anySliceToRef(readMap[dev.RefTag+"[0]"])
			if err != nil {
				return out, fmt.Errorf("erro convertendo %s: %w", dev.RefTag, err)
			}
			out.Com = com
			out.Ref = ref
			return out, nil
		}
		log.Printf("[%s] erro lendo PLC em bloco via ReadMap (%v), reconectando...", dev.Name, err)
		p.markDisconnectedLocked()
		p.mu.Unlock()
		if err := p.reconnectWithRetry(ctx); err != nil {
			return out, err
		}
	}
}

type ModbusDeviceClient struct {
	dev    DeviceConfig
	mu     sync.Mutex
	conn   net.Conn
	txid   uint16
	logger *log.Logger
}

func NewModbusDeviceClient(dev DeviceConfig) *ModbusDeviceClient {
	prefix := fmt.Sprintf("[%s][MODBUS] ", dev.Name)
	return &ModbusDeviceClient{dev: dev, logger: log.New(os.Stdout, prefix, log.LstdFlags|log.Lmicroseconds)}
}

func (c *ModbusDeviceClient) connectLocked() error {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	dialer := net.Dialer{Timeout: modbusConnectTimeout, Control: controlBindToDevice(virtualInterface)}
	conn, err := dialer.Dial("tcp4", fmt.Sprintf("%s:%d", c.dev.VirtualIP, modbusPort))
	if err != nil {
		return err
	}
	c.conn = conn
	c.logger.Printf("conectado em %s:%d | unitID=%d", c.dev.VirtualIP, modbusPort, c.dev.UnitID)
	return nil
}

func (c *ModbusDeviceClient) ensureConnectedLocked() error {
	if c.conn != nil {
		return nil
	}
	return c.connectLocked()
}

func (c *ModbusDeviceClient) markDisconnectedLocked() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (c *ModbusDeviceClient) reconnectWithRetry(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		c.mu.Lock()
		err := c.connectLocked()
		c.mu.Unlock()
		if err == nil {
			return nil
		}
		c.logger.Printf("falha ao reconectar: %v", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnectInterval):
		}
	}
}

func (c *ModbusDeviceClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.markDisconnectedLocked()
}

func (c *ModbusDeviceClient) sendPDU(ctx context.Context, pdu []byte) ([]byte, error) {
	for {
		c.mu.Lock()
		if err := c.ensureConnectedLocked(); err != nil {
			c.mu.Unlock()
			if err := c.reconnectWithRetry(ctx); err != nil {
				return nil, err
			}
			continue
		}

		c.txid++
		txid := c.txid
		adu := buildModbusADU(txid, c.dev.UnitID, pdu)
		_ = c.conn.SetWriteDeadline(time.Now().Add(modbusWriteTimeout))
		_, err := c.conn.Write(adu)
		if err != nil {
			c.logger.Printf("erro escrevendo ADU: %v", err)
			c.markDisconnectedLocked()
			c.mu.Unlock()
			if err := c.reconnectWithRetry(ctx); err != nil {
				return nil, err
			}
			continue
		}

		_ = c.conn.SetReadDeadline(time.Now().Add(modbusReadTimeout))
		respPDU, err := readModbusADU(c.conn, txid, c.dev.UnitID)
		if err != nil {
			c.logger.Printf("erro lendo resposta ADU: %v", err)
			c.markDisconnectedLocked()
			c.mu.Unlock()
			if err := c.reconnectWithRetry(ctx); err != nil {
				return nil, err
			}
			continue
		}
		c.mu.Unlock()
		return respPDU, nil
	}
}

func (c *ModbusDeviceClient) ReadStatusFeedback(ctx context.Context) (Payload, error) {
	var out Payload
	// O device_router expõe Status+Feedback na área Input Registers.
	// Portanto, a leitura correta é FC04 (Read Input Registers), addr 0, qty 44.
	pdu := []byte{0x04, 0x00, 0x00, 0x00, 0x2c}
	resp, err := c.sendPDU(ctx, pdu)
	if err != nil {
		return out, err
	}
	if len(resp) < 2 || resp[0] != 0x04 {
		return out, fmt.Errorf("resposta inválida FC04: %x", resp)
	}
	if len(resp) < 2+88 || int(resp[1]) != 88 {
		return out, fmt.Errorf("byte count inválido em FC04: len=%d count=%d", len(resp), resp[1])
	}
	regs := bytesToRegs(resp[2 : 2+88])
	payload, err := regsToStatusFeedback(regs)
	if err != nil {
		return out, err
	}
	return payload, nil
}

func (c *ModbusDeviceClient) WriteCommand(ctx context.Context, cmd CommandPayload) error {
	regs := commandToRegs(cmd)
	byteCount := len(regs) * 2
	pdu := make([]byte, 0, 6+1+byteCount)
	pdu = append(pdu, 0x10, 0x00, 0x00, 0x00, 0x2c, byte(byteCount))
	for _, r := range regs {
		pdu = binary.BigEndian.AppendUint16(pdu, r)
	}
	resp, err := c.sendPDU(ctx, pdu)
	if err != nil {
		return err
	}
	if len(resp) != 5 || resp[0] != 0x10 {
		return fmt.Errorf("resposta inválida FC16: %x", resp)
	}
	qty := binary.BigEndian.Uint16(resp[3:5])
	if qty != modbusQtyAll {
		return fmt.Errorf("quantidade confirmada inválida no FC16: %d", qty)
	}
	return nil
}

func runVirtualModbusLoop(ctx context.Context, dev DeviceConfig, mem *SharedMemory, cycle time.Duration, writer *InteractionCSVWriter) error {
	client := NewModbusDeviceClient(dev)
	defer client.Close()

	ticker := time.NewTicker(cycle)
	defer ticker.Stop()
	var loopIndex uint64
	log.Printf("[%s][VIRTUAL-MODBUS] iniciado | ip=%s | port=%d | unitID=%d | cycle=%s", dev.Name, dev.VirtualIP, modbusPort, dev.UnitID, cycle)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			loopIndex++

			readStart := time.Now()
			payload, err := client.ReadStatusFeedback(ctx)
			readDur := time.Since(readStart)
			if err != nil {
				log.Printf("[%s][VIRTUAL-MODBUS][READ] erro: %v", dev.Name, err)
				if writer != nil {
					_ = writer.Write(InteractionRecord{
						Timestamp: time.Now(), LoopType: "DEVICE_MODBUS", LoopIndex: loopIndex, Device: dev.Name,
						Protocol: "Modbus/TCP", FunctionCode: "FC04", Operation: "READ_STATUS_FEEDBACK",
						Direction: "from_device", Target: fmt.Sprintf("%s:%d/unit/%d", dev.VirtualIP, modbusPort, dev.UnitID),
						Status: "error", Duration: readDur, Details: fmt.Sprintf("addr=%d qty=%d", modbusAddrStart, modbusQtyAll), Error: err.Error(),
					})
				}
			} else {
				mem.UpdateRx(dev.Name, payload, fmt.Sprintf("modbus://%s:%d/unit/%d", dev.VirtualIP, modbusPort, dev.UnitID))
				if writer != nil {
					_ = writer.Write(InteractionRecord{
						Timestamp: time.Now(), LoopType: "DEVICE_MODBUS", LoopIndex: loopIndex, Device: dev.Name,
						Protocol: "Modbus/TCP", FunctionCode: "FC04", Operation: "READ_STATUS_FEEDBACK",
						Direction: "from_device", Target: fmt.Sprintf("%s:%d/unit/%d", dev.VirtualIP, modbusPort, dev.UnitID),
						Status: "ok", Duration: readDur, Details: fmt.Sprintf("addr=%d qty=%d", modbusAddrStart, modbusQtyAll),
					})
				}
			}

			cmd, _, version, hasNew, _ := mem.ReadTx(dev.Name)
			if !hasNew {
				continue
			}

			writeStart := time.Now()
			err = client.WriteCommand(ctx, cmd)
			writeDur := time.Since(writeStart)
			if err != nil {
				log.Printf("[%s][VIRTUAL-MODBUS][WRITE] erro: %v", dev.Name, err)
				if writer != nil {
					_ = writer.Write(InteractionRecord{
						Timestamp: time.Now(), LoopType: "DEVICE_MODBUS", LoopIndex: loopIndex, Device: dev.Name,
						Protocol: "Modbus/TCP", FunctionCode: "FC16", Operation: "WRITE_COMMAND_REF",
						Direction: "to_device", Target: fmt.Sprintf("%s:%d/unit/%d", dev.VirtualIP, modbusPort, dev.UnitID),
						Status: "error", Duration: writeDur, Details: fmt.Sprintf("addr=%d qty=%d version=%d", modbusAddrStart, modbusQtyAll, version), Error: err.Error(),
					})
				}
				continue
			}
			mem.MarkTxSent(dev.Name, version)
			if writer != nil {
				_ = writer.Write(InteractionRecord{
					Timestamp: time.Now(), LoopType: "DEVICE_MODBUS", LoopIndex: loopIndex, Device: dev.Name,
					Protocol: "Modbus/TCP", FunctionCode: "FC16", Operation: "WRITE_COMMAND_REF",
					Direction: "to_device", Target: fmt.Sprintf("%s:%d/unit/%d", dev.VirtualIP, modbusPort, dev.UnitID),
					Status: "ok", Duration: writeDur, Details: fmt.Sprintf("addr=%d qty=%d version=%d", modbusAddrStart, modbusQtyAll, version),
				})
			}
		}
	}
}

func runPLCScan(ctx context.Context, devices []DeviceConfig, plc *PLCBridge, mem *SharedMemory, timeout time.Duration, readLatency time.Duration, period time.Duration, plcWriter *InteractionCSVWriter) error {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	var scanIndex uint64
	log.Printf("[PLC-SCAN] iniciado | plcCycle=%s | readLatency=%s | scanPeriod=%s | stale=%s | devices=%d", plcCycle, readLatency, period, timeout, len(devices))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case tickTime := <-ticker.C:
			scanIndex++
			totalStart := time.Now()
			metrics := ScanMetrics{Timestamp: time.Now(), ScanIndex: scanIndex, Budget: period}

			writeStart := time.Now()
			for _, dev := range devices {
				rxPayload, rxTime, _, src := mem.ReadRx(dev.Name)
				opStart := time.Now()
				staleWrite := rxTime.IsZero() || time.Since(rxTime) > timeout
				var writePayload Payload
				details := fmt.Sprintf("statusTag=%s[0];feedbackTag=%s[0];source=%s", dev.StatusTag, dev.FeedbackTag, src)
				if !staleWrite {
					writePayload = rxPayload
				} else {
					writePayload.Status[0] = 0
					writePayload.Status[1] = -1
					for i := 0; i < 20; i++ {
						writePayload.Feedback[i] = 0
					}
					details += ";stale_fallback=true"
				}
				err := plc.WriteStatusFeedback(ctx, dev, writePayload)
				opDur := time.Since(opStart)
				if err != nil {
					log.Printf("[%s][PLC-SCAN][WRITE] erro escrevendo Status/Feedback: %v", dev.Name, err)
					if plcWriter != nil {
						_ = plcWriter.Write(InteractionRecord{
							Timestamp: time.Now(), LoopType: "PLC_ETHERNETIP", LoopIndex: scanIndex, Device: dev.Name,
							Protocol: "EtherNet/IP", FunctionCode: "TAG_WRITE", Operation: "WRITE_STATUS_FEEDBACK",
							Direction: "to_plc", Target: fmt.Sprintf("%s[0]|%s[0]", dev.StatusTag, dev.FeedbackTag),
							Status: "error", Duration: opDur, Details: details, Error: err.Error(),
						})
					}
				} else if plcWriter != nil {
					status := "ok"
					if staleWrite {
						status = "stale_write"
					}
					_ = plcWriter.Write(InteractionRecord{
						Timestamp: time.Now(), LoopType: "PLC_ETHERNETIP", LoopIndex: scanIndex, Device: dev.Name,
						Protocol: "EtherNet/IP", FunctionCode: "TAG_WRITE", Operation: "WRITE_STATUS_FEEDBACK",
						Direction: "to_plc", Target: fmt.Sprintf("%s[0]|%s[0]", dev.StatusTag, dev.FeedbackTag),
						Status: status, Duration: opDur, Details: details,
					})
				}
				metrics.DevicesWritten++
			}
			metrics.WritePhase = time.Since(writeStart)

			latencyStart := time.Now()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(readLatency):
			}
			metrics.LatencyPhase = time.Since(latencyStart)

			readStart := time.Now()
			for _, dev := range devices {
				opStart := time.Now()
				cmd, err := plc.ReadComRef(ctx, dev)
				opDur := time.Since(opStart)
				if err != nil {
					log.Printf("[%s][PLC-SCAN][READ] erro lendo Com/Ref: %v", dev.Name, err)
					if plcWriter != nil {
						_ = plcWriter.Write(InteractionRecord{
							Timestamp: time.Now(), LoopType: "PLC_ETHERNETIP", LoopIndex: scanIndex, Device: dev.Name,
							Protocol: "EtherNet/IP", FunctionCode: "TAG_READ", Operation: "READ_COM_REF",
							Direction: "from_plc", Target: fmt.Sprintf("%s[0]|%s[0]", dev.ComTag, dev.RefTag),
							Status: "error", Duration: opDur, Details: "count=2+20", Error: err.Error(),
						})
					}
					continue
				}
				changed := mem.UpdateTx(dev.Name, cmd)
				if plcWriter != nil {
					_ = plcWriter.Write(InteractionRecord{
						Timestamp: time.Now(), LoopType: "PLC_ETHERNETIP", LoopIndex: scanIndex, Device: dev.Name,
						Protocol: "EtherNet/IP", FunctionCode: "TAG_READ", Operation: "READ_COM_REF",
						Direction: "from_plc", Target: fmt.Sprintf("%s[0]|%s[0]", dev.ComTag, dev.RefTag),
						Status: "ok", Duration: opDur, Details: fmt.Sprintf("count=2+20;changed=%t", changed),
					})
				}
				metrics.DevicesRead++
			}
			metrics.ReadPhase = time.Since(readStart)

			metrics.TotalPhase = time.Since(totalStart)
			metrics.Overrun = metrics.TotalPhase > metrics.Budget
			if metrics.Overrun {
				metrics.OverrunBy = metrics.TotalPhase - metrics.Budget
			}

			if plcWriter != nil {
				_ = plcWriter.Write(InteractionRecord{
					Timestamp: time.Now(), LoopType: "PLC_SCAN_SUMMARY", LoopIndex: scanIndex, Device: "ALL",
					Protocol: "EtherNet/IP", FunctionCode: "SCAN", Operation: "SCAN_SUMMARY",
					Direction: "internal", Target: "PLC_SCAN", Status: func() string {
						if metrics.Overrun {
							return "overrun"
						}
						return "ok"
					}(),
					Details:   fmt.Sprintf("devices_written=%d;devices_read=%d;tick=%s", metrics.DevicesWritten, metrics.DevicesRead, tickTime.Format(time.RFC3339Nano)),
					ScanWrite: metrics.WritePhase, ScanLatency: metrics.LatencyPhase, ScanRead: metrics.ReadPhase,
					ScanTotal: metrics.TotalPhase, Budget: metrics.Budget, Overrun: metrics.Overrun, OverrunBy: metrics.OverrunBy,
				})
			}

			if metrics.Overrun {
				log.Printf("[PLC-SCAN][#%d] OVERUN | total=%s | budget=%s | over=%s | write=%s | latency=%s | read=%s | wrote=%d | read_ok=%d | tick=%s",
					metrics.ScanIndex, metrics.TotalPhase, metrics.Budget, metrics.OverrunBy, metrics.WritePhase, metrics.LatencyPhase, metrics.ReadPhase, metrics.DevicesWritten, metrics.DevicesRead, tickTime.Format(time.RFC3339Nano))
			} else {
				log.Printf("[PLC-SCAN][#%d] OK | total=%s | budget=%s | write=%s | latency=%s | read=%s | wrote=%d | read_ok=%d",
					metrics.ScanIndex, metrics.TotalPhase, metrics.Budget, metrics.WritePhase, metrics.LatencyPhase, metrics.ReadPhase, metrics.DevicesWritten, metrics.DevicesRead)
			}

		}
	}
}

func buildModbusADU(txid uint16, unitID uint8, pdu []byte) []byte {
	adu := make([]byte, 0, 7+len(pdu))
	adu = binary.BigEndian.AppendUint16(adu, txid)
	adu = binary.BigEndian.AppendUint16(adu, 0)
	adu = binary.BigEndian.AppendUint16(adu, uint16(len(pdu)+1))
	adu = append(adu, unitID)
	adu = append(adu, pdu...)
	return adu
}

func readModbusADU(r io.Reader, expectedTxID uint16, expectedUnitID uint8) ([]byte, error) {
	header := make([]byte, 7)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	txid := binary.BigEndian.Uint16(header[0:2])
	proto := binary.BigEndian.Uint16(header[2:4])
	length := binary.BigEndian.Uint16(header[4:6])
	unitID := header[6]
	if txid != expectedTxID {
		return nil, fmt.Errorf("transaction id inválido: got=%d expected=%d", txid, expectedTxID)
	}
	if proto != 0 {
		return nil, fmt.Errorf("protocol id inválido: %d", proto)
	}
	if unitID != expectedUnitID {
		return nil, fmt.Errorf("unit id inválido: got=%d expected=%d", unitID, expectedUnitID)
	}
	if length < 1 {
		return nil, fmt.Errorf("length inválido: %d", length)
	}
	pduLen := int(length) - 1
	pdu := make([]byte, pduLen)
	if _, err := io.ReadFull(r, pdu); err != nil {
		return nil, err
	}
	if len(pdu) > 0 && (pdu[0]&0x80) != 0 {
		if len(pdu) >= 2 {
			return nil, fmt.Errorf("exception modbus fc=0x%02x code=0x%02x", pdu[0], pdu[1])
		}
		return nil, fmt.Errorf("exception modbus incompleta: %x", pdu)
	}
	return pdu, nil
}

func bytesToRegs(b []byte) []uint16 {
	regs := make([]uint16, 0, len(b)/2)
	for i := 0; i+1 < len(b); i += 2 {
		regs = append(regs, binary.BigEndian.Uint16(b[i:i+2]))
	}
	return regs
}

func regsToStatusFeedback(regs []uint16) (Payload, error) {
	var p Payload
	if len(regs) != modbusQtyAll {
		return p, fmt.Errorf("quantidade inválida de registradores RX: %d", len(regs))
	}
	for i := 0; i < 2; i++ {
		p.Status[i] = int32(twoRegsToU32LowWordFirst(regs[i*2], regs[i*2+1]))
	}
	base := 4
	for i := 0; i < 20; i++ {
		idx := base + i*2
		p.Feedback[i] = u32ToFloat32(twoRegsToU32LowWordFirst(regs[idx], regs[idx+1]))
	}
	return p, nil
}

func commandToRegs(c CommandPayload) []uint16 {
	regs := make([]uint16, 0, modbusQtyAll)
	for i := 0; i < 2; i++ {
		lo, hi := u32ToTwoRegsLowWordFirst(uint32(c.Com[i]))
		regs = append(regs, lo, hi)
	}
	for i := 0; i < 20; i++ {
		lo, hi := u32ToTwoRegsLowWordFirst(float32ToU32(c.Ref[i]))
		regs = append(regs, lo, hi)
	}
	return regs
}

func twoRegsToU32LowWordFirst(lo, hi uint16) uint32 {
	return uint32(lo) | (uint32(hi) << 16)
}

func u32ToTwoRegsLowWordFirst(v uint32) (uint16, uint16) {
	return uint16(v & 0xFFFF), uint16((v >> 16) & 0xFFFF)
}

func float32ToU32(v float32) uint32 { return math.Float32bits(v) }
func u32ToFloat32(v uint32) float32 { return math.Float32frombits(v) }

func getInterfaceIPv4(ifName string) (string, error) {
	iface, err := net.InterfaceByName(ifName)
	if err != nil {
		return "", err
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok {
			ip := ipNet.IP.To4()
			if ip != nil {
				return ip.String(), nil
			}
		}
	}
	return "", fmt.Errorf("nenhum IPv4 encontrado em %s", ifName)
}

func controlBindToDevice(ifName string) func(string, string, syscall.RawConn) error {
	return func(_, _ string, c syscall.RawConn) error {
		var innerErr error
		err := c.Control(func(fd uintptr) {
			innerErr = syscall.SetsockoptString(int(fd), syscall.SOL_SOCKET, syscall.SO_BINDTODEVICE, ifName)
		})
		if err != nil {
			return err
		}
		return innerErr
	}
}
