package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/simonvetter/modbus"
)

const (
	realInterface    = "eth0"
	virtualInterface = "eth1"

	opalRealIP = "192.168.0.16"

	udpBufferSize     = 2048
	tcpReadTimeout    = 3 * time.Second
	tcpWriteTimeout   = 3 * time.Second
	tcpConnectTimeout = 3 * time.Second
	tcpIdleTimeout    = 300 * time.Second
	udpReadTimeout    = 1 * time.Second

	sendCycle    = 50 * time.Millisecond
	staleTimeout = 1 * time.Second

	// Parâmetros de retry do envio de comando ao OPAL.
	// Ajuste estes dois valores conforme o comportamento desejado.
	maxCommandSendRetries = 3
	commandRetryDelay     = 20 * time.Millisecond

	transportUDP  = "UDP"
	transportTCP  = "TCP"
	transportBoth = "BOTH"

	// Transporte usado para RECEBER feedback/status do OPAL na eth0.
	// Escolha apenas uma opção: transportUDP ou transportTCP.
	feedbackReceiveTransport = transportUDP

	// Transporte usado para ENVIAR comandos ao OPAL na eth0.
	commandTransport = transportTCP

	sendModeAlways   = "ALWAYS"
	sendModeOnChange = "ON_CHANGE"

	commandSendMode = sendModeOnChange

	modbusPort = 502

	enableUnifiedMetricsCSV = true
	unifiedMetricsCSVPath   = "./device_router_metrics.csv"
)

type DeviceConfig struct {
	Name         string
	ModbusUnitID byte
	VirtualIP    string
	RealIP       string
	TxPort       int
	RxPort       int
}

var deviceTable = map[string]DeviceConfig{
	"192.168.1.30": {Name: "POI", ModbusUnitID: 0, VirtualIP: "192.168.1.30", RealIP: "192.168.0.30", TxPort: 23001, RxPort: 23002},
	"192.168.1.31": {Name: "BESS", ModbusUnitID: 1, VirtualIP: "192.168.1.31", RealIP: "192.168.0.31", TxPort: 23003, RxPort: 23004},
	"192.168.1.32": {Name: "GMG", ModbusUnitID: 2, VirtualIP: "192.168.1.32", RealIP: "192.168.0.32", TxPort: 23005, RxPort: 23006},
	"192.168.1.33": {Name: "PV", ModbusUnitID: 3, VirtualIP: "192.168.1.33", RealIP: "192.168.0.33", TxPort: 23007, RxPort: 23008},
	"192.168.1.34": {Name: "LD0", ModbusUnitID: 4, VirtualIP: "192.168.1.34", RealIP: "192.168.0.34", TxPort: 23009, RxPort: 23010},
	"192.168.1.35": {Name: "LD1", ModbusUnitID: 5, VirtualIP: "192.168.1.35", RealIP: "192.168.0.35", TxPort: 23011, RxPort: 23012},
}

type Payload struct {
	Status   [2]int32
	Feedback [20]float32
}

type CommandPayload struct {
	Com [2]int32
	Ref [20]float32
}

type RxSlot struct {
	Payload      Payload
	LastUpdate   time.Time
	Version      uint64
	HasNewData   bool
	LastSource   string
	LastProtocol string
}

type TxSlot struct {
	Payload               CommandPayload
	LastUpdate            time.Time
	Version               uint64
	LastSentVersion       uint64
	LastCompletedVersion  uint64
	LastCompletedSuccess  bool
	HasNewData            bool
}

type InterfaceMemory struct {
	mu sync.RWMutex
	Rx RxSlot
	Tx TxSlot
}

func NewInterfaceMemory() *InterfaceMemory { return &InterfaceMemory{} }

func (m *InterfaceMemory) UpdateRx(p Payload, src string, protocol string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Rx.Payload = p
	m.Rx.LastUpdate = time.Now()
	m.Rx.Version++
	m.Rx.HasNewData = true
	m.Rx.LastSource = src
	m.Rx.LastProtocol = protocol
}

func (m *InterfaceMemory) ReadRx() (Payload, bool, time.Time, uint64, string, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Rx.Payload, m.Rx.HasNewData, m.Rx.LastUpdate, m.Rx.Version, m.Rx.LastSource, m.Rx.LastProtocol
}

func (m *InterfaceMemory) UpdateTx(p CommandPayload) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Tx.Payload = p
	m.Tx.LastUpdate = time.Now()
	m.Tx.Version++
	m.Tx.HasNewData = true
}

func (m *InterfaceMemory) ReadTx() (CommandPayload, bool, uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Tx.Payload, m.Tx.HasNewData, m.Tx.Version
}

func (m *InterfaceMemory) MarkTxSent(version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Tx.LastSentVersion = version
	if m.Tx.Version == version {
		m.Tx.HasNewData = false
	}
}

func (m *InterfaceMemory) MarkTxFailed(version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Tx.Version == version {
		m.Tx.HasNewData = false
	}
}

type CommandCycleMetric struct {
	Timestamp         time.Time
	Device            string
	CycleIndex        uint64
	Transport         string
	HasNewTX          bool
	ShouldSend        bool
	SendAttempted     bool
	SendSuccess       bool
	RetryAttempts     int
	CompletionState   string
	LastError         string
	SendDuration      time.Duration
	TxVersion         uint64
	PayloadBytes      int
	RxFresh           bool
	RxAge             time.Duration
	RxVersion         uint64
	LastSource        string
	RxProtocol        string
	RxUpdatedAt       time.Time
	MemoryUpdateAge   time.Duration
	PhysicalDirection string
}


type UnifiedMetric struct {
	Timestamp         time.Time
	EventType         string
	Device            string
	InterfaceName     string
	Direction         string
	Protocol          string
	PeerAddr          string
	Port              int
	PayloadBytes      int
	Duration          time.Duration
	MemUpdateAt       time.Time
	MemoryAge         time.Duration
	RxVersion         uint64
	TxVersion         uint64
	CycleIndex        uint64
	Transport         string
	HasNewTX          bool
	ShouldSend        bool
	SendAttempted     bool
	SendSuccess       bool
	RetryAttempts     int
	CompletionState   string
	LastError         string
	RxFresh           bool
	RxAge             time.Duration
	LastSource        string
	RxProtocol        string
	RxUpdatedAt       time.Time
	MemoryUpdateAge   time.Duration
	ClientAddr        string
	UnitID            uint8
	Area              string
	FunctionCode      uint8
	IsWrite           bool
	Addr              uint16
	Quantity          uint16
	Status            string
	Error             string
}

type UnifiedMetricsWriter struct {
	enabled      bool
	path         string
	mu           sync.Mutex
	file         *os.File
	writer       *csv.Writer
	lastEventAt  map[string]time.Time
	lastInterval map[string]time.Duration
	lastDuration map[string]time.Duration
}

func NewUnifiedMetricsWriter(enabled bool, path string) (*UnifiedMetricsWriter, error) {
	w := &UnifiedMetricsWriter{
		enabled:      enabled,
		path:         path,
		lastEventAt:  make(map[string]time.Time),
		lastInterval: make(map[string]time.Duration),
		lastDuration: make(map[string]time.Duration),
	}
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
			"timestamp","event_type","device","interface_name","direction","protocol","peer_addr","port",
			"payload_bytes","duration_ms","throughput_mbps","inter_event_ms","jitter_ms","duration_jitter_ms",
			"mem_update_at","memory_age_ms","rx_version","tx_version",
			"cycle_index","transport","has_new_tx","should_send","send_attempted","send_success","retry_attempts","completion_state","last_error",
			"rx_fresh","rx_age_ms","last_source","rx_protocol","rx_updated_at","memory_update_age_ms",
			"client_addr","unit_id","area","function_code","is_write","addr","quantity","status","error",
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

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func metricKey(m UnifiedMetric) string {
	return m.Device + "|" + m.InterfaceName + "|" + m.Direction + "|" + m.Protocol + "|" + m.EventType
}

func (w *UnifiedMetricsWriter) Write(m UnifiedMetric) error {
	if !w.enabled {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now()
	}

	key := metricKey(m)
	var interEvent time.Duration
	var jitter time.Duration
	var durationJitter time.Duration
	if lastTs, ok := w.lastEventAt[key]; ok {
		interEvent = m.Timestamp.Sub(lastTs)
		if lastInt, ok := w.lastInterval[key]; ok {
			jitter = absDuration(interEvent - lastInt)
		}
		w.lastInterval[key] = interEvent
	}
	w.lastEventAt[key] = m.Timestamp

	if lastDur, ok := w.lastDuration[key]; ok {
		durationJitter = absDuration(m.Duration - lastDur)
	}
	if m.Duration > 0 {
		w.lastDuration[key] = m.Duration
	}

	throughputMbps := 0.0
	if m.Duration > 0 && m.PayloadBytes > 0 {
		throughputMbps = (float64(m.PayloadBytes) * 8.0) / m.Duration.Seconds() / 1_000_000.0
	}

	row := []string{
		m.Timestamp.Format(time.RFC3339Nano),
		m.EventType,
		m.Device,
		m.InterfaceName,
		m.Direction,
		m.Protocol,
		m.PeerAddr,
		strconv.Itoa(m.Port),
		strconv.Itoa(m.PayloadBytes),
		fmt.Sprintf("%.6f", float64(m.Duration.Microseconds())/1000.0),
		fmt.Sprintf("%.6f", throughputMbps),
		fmt.Sprintf("%.6f", float64(interEvent.Microseconds())/1000.0),
		fmt.Sprintf("%.6f", float64(jitter.Microseconds())/1000.0),
		fmt.Sprintf("%.6f", float64(durationJitter.Microseconds())/1000.0),
		m.MemUpdateAt.Format(time.RFC3339Nano),
		fmt.Sprintf("%.6f", float64(m.MemoryAge.Microseconds())/1000.0),
		strconv.FormatUint(m.RxVersion, 10),
		strconv.FormatUint(m.TxVersion, 10),
		strconv.FormatUint(m.CycleIndex, 10),
		m.Transport,
		strconv.FormatBool(m.HasNewTX),
		strconv.FormatBool(m.ShouldSend),
		strconv.FormatBool(m.SendAttempted),
		strconv.FormatBool(m.SendSuccess),
		strconv.Itoa(m.RetryAttempts),
		m.CompletionState,
		m.LastError,
		strconv.FormatBool(m.RxFresh),
		fmt.Sprintf("%.6f", float64(m.RxAge.Microseconds())/1000.0),
		m.LastSource,
		m.RxProtocol,
		m.RxUpdatedAt.Format(time.RFC3339Nano),
		fmt.Sprintf("%.6f", float64(m.MemoryUpdateAge.Microseconds())/1000.0),
		m.ClientAddr,
		strconv.Itoa(int(m.UnitID)),
		m.Area,
		strconv.Itoa(int(m.FunctionCode)),
		strconv.FormatBool(m.IsWrite),
		strconv.Itoa(int(m.Addr)),
		strconv.Itoa(int(m.Quantity)),
		m.Status,
		m.Error,
	}
	if err := w.writer.Write(row); err != nil {
		return err
	}
	w.writer.Flush()
	return w.writer.Error()
}

func (w *UnifiedMetricsWriter) Close() error {
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


type CommandMetricsWriter struct {
	enabled bool
	path    string
	mu      sync.Mutex
	file    *os.File
	writer  *csv.Writer
}

func NewCommandMetricsWriter(enabled bool, path string) (*CommandMetricsWriter, error) {
	w := &CommandMetricsWriter{enabled: enabled, path: path}
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
		if err := w.writer.Write([]string{"timestamp", "device", "cycle_index", "transport", "physical_direction", "has_new_tx", "should_send", "send_attempted", "send_success", "retry_attempts", "completion_state", "last_error", "send_duration_ms", "tx_version", "payload_bytes", "rx_fresh", "rx_age_ms", "rx_version", "last_source", "rx_protocol", "rx_updated_at", "memory_update_age_ms"}); err != nil {
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

func (w *CommandMetricsWriter) Write(m CommandCycleMetric) error {
	if !w.enabled {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	row := []string{
		m.Timestamp.Format(time.RFC3339Nano),
		m.Device,
		strconv.FormatUint(m.CycleIndex, 10),
		m.Transport,
		m.PhysicalDirection,
		strconv.FormatBool(m.HasNewTX),
		strconv.FormatBool(m.ShouldSend),
		strconv.FormatBool(m.SendAttempted),
		strconv.FormatBool(m.SendSuccess),
		strconv.Itoa(m.RetryAttempts),
		m.CompletionState,
		m.LastError,
		fmt.Sprintf("%.6f", float64(m.SendDuration.Microseconds())/1000.0),
		strconv.FormatUint(m.TxVersion, 10),
		strconv.Itoa(m.PayloadBytes),
		strconv.FormatBool(m.RxFresh),
		fmt.Sprintf("%.6f", float64(m.RxAge.Microseconds())/1000.0),
		strconv.FormatUint(m.RxVersion, 10),
		m.LastSource,
		m.RxProtocol,
		m.RxUpdatedAt.Format(time.RFC3339Nano),
		fmt.Sprintf("%.6f", float64(m.MemoryUpdateAge.Microseconds())/1000.0),
	}
	if err := w.writer.Write(row); err != nil {
		return err
	}
	w.writer.Flush()
	return w.writer.Error()
}

func (w *CommandMetricsWriter) Close() error {
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


type PhysicalIOMetric struct {
	Timestamp       time.Time
	Device          string
	Direction       string
	Protocol        string
	PeerAddr        string
	Port            int
	PayloadBytes    int
	MemUpdateAt     time.Time
	MemoryAge       time.Duration
	RxVersion       uint64
	TxVersion       uint64
	Status          string
	Error           string
}

type PhysicalIOMetricsWriter struct {
	enabled bool
	path    string
	mu      sync.Mutex
	file    *os.File
	writer  *csv.Writer
}

func NewPhysicalIOMetricsWriter(enabled bool, path string) (*PhysicalIOMetricsWriter, error) {
	w := &PhysicalIOMetricsWriter{enabled: enabled, path: path}
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
		if err := w.writer.Write([]string{"timestamp","device","direction","protocol","peer_addr","port","payload_bytes","mem_update_at","memory_age_ms","rx_version","tx_version","status","error"}); err != nil {
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

func (w *PhysicalIOMetricsWriter) Write(m PhysicalIOMetric) error {
	if !w.enabled {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	row := []string{
		m.Timestamp.Format(time.RFC3339Nano),
		m.Device,
		m.Direction,
		m.Protocol,
		m.PeerAddr,
		strconv.Itoa(m.Port),
		strconv.Itoa(m.PayloadBytes),
		m.MemUpdateAt.Format(time.RFC3339Nano),
		fmt.Sprintf("%.6f", float64(m.MemoryAge.Microseconds())/1000.0),
		strconv.FormatUint(m.RxVersion, 10),
		strconv.FormatUint(m.TxVersion, 10),
		m.Status,
		m.Error,
	}
	if err := w.writer.Write(row); err != nil {
		return err
	}
	w.writer.Flush()
	return w.writer.Error()
}

func (w *PhysicalIOMetricsWriter) Close() error {
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

type ModbusRequestMetric struct {
	Timestamp    time.Time
	Device       string
	ClientAddr   string
	UnitID       uint8
	Area         string
	FunctionCode uint8
	IsWrite      bool
	Addr         uint16
	Quantity     uint16
	Duration     time.Duration
	Status       string
	Error        string
}

func modbusFunctionCodeHolding(isWrite bool) byte {
	if isWrite {
		return 16 // Write Multiple Holding Registers
	}
	return 3 // Read Holding Registers
}

func modbusFunctionCodeInput() byte {
	return 4 // Read Input Registers
}

type ModbusRequestWriter struct {
	enabled bool
	path    string
	mu      sync.Mutex
	file    *os.File
	writer  *csv.Writer
}

func NewModbusRequestWriter(enabled bool, path string) (*ModbusRequestWriter, error) {
	w := &ModbusRequestWriter{enabled: enabled, path: path}
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
		if err := w.writer.Write([]string{"timestamp", "device", "client_addr", "unit_id", "area", "function_code", "is_write", "addr", "quantity", "duration_ms", "status", "error"}); err != nil {
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

func (w *ModbusRequestWriter) Write(m ModbusRequestMetric) error {
	if !w.enabled {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	row := []string{
		m.Timestamp.Format(time.RFC3339Nano),
		m.Device,
		m.ClientAddr,
		strconv.Itoa(int(m.UnitID)),
		m.Area,
		strconv.Itoa(int(m.FunctionCode)),
		strconv.FormatBool(m.IsWrite),
		strconv.Itoa(int(m.Addr)),
		strconv.Itoa(int(m.Quantity)),
		fmt.Sprintf("%.6f", float64(m.Duration.Microseconds())/1000.0),
		m.Status,
		m.Error,
	}
	if err := w.writer.Write(row); err != nil {
		return err
	}
	w.writer.Flush()
	return w.writer.Error()
}

func (w *ModbusRequestWriter) Close() error {
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

	realIP, err := getInterfaceIPv4(realInterface)
	if err != nil {
		log.Fatalf("erro ao obter IP de %s: %v", realInterface, err)
	}
	virtualIP, err := getInterfaceIPv4(virtualInterface)
	if err != nil {
		log.Fatalf("erro ao obter IP de %s: %v", virtualInterface, err)
	}
	dev, ok := deviceTable[virtualIP]
	if !ok {
		log.Fatalf("IP virtual %s não reconhecido", virtualIP)
	}

	metricsWriter, err := NewUnifiedMetricsWriter(enableUnifiedMetricsCSV, unifiedMetricsCSVPath)
	if err != nil {
		log.Fatalf("erro abrindo CSV unificado de métricas: %v", err)
	}
	defer metricsWriter.Close()

	log.Printf("Container identificado como %s | UnitID=%d | eth0=%s | eth1=%s", dev.Name, dev.ModbusUnitID, realIP, virtualIP)
	log.Printf("[%s] feedbackReceiveTransport=%s | commandTransport=%s", dev.Name, feedbackReceiveTransport, commandTransport)
	log.Printf("[%s] CSV unificado=%t path=%s", dev.Name, enableUnifiedMetricsCSV, unifiedMetricsCSVPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mem := NewInterfaceMemory()
	var wg sync.WaitGroup
	errCh := make(chan error, 8)

	switch feedbackReceiveTransport {
	case transportUDP:
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runUDPReceiver(ctx, realIP, dev, mem, metricsWriter); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("[%s][UDP-RX] %w", dev.Name, err)
			}
		}()
	case transportTCP:
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runTCPReceiver(ctx, realIP, dev, mem, metricsWriter); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("[%s][TCP-RX] %w", dev.Name, err)
			}
		}()
	default:
		log.Fatalf("[%s] feedbackReceiveTransport inválido: %s", dev.Name, feedbackReceiveTransport)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runPhysicalCommandSender(ctx, dev, mem, commandTransport, commandSendMode, sendCycle, metricsWriter); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("[%s][CMD-TX] %w", dev.Name, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runModbusServer(ctx, dev, mem, metricsWriter); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("[%s][MODBUS] %w", dev.Name, err)
		}
	}()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		log.Printf("encerrando device interface")
	case err := <-errCh:
		if err != nil {
			log.Printf("erro: %v", err)
			stop()
		}
	}

	wg.Wait()
}

type modbusHandler struct {
	mem          *InterfaceMemory
	device       string
	modbusUnitID byte
	metricsWriter *UnifiedMetricsWriter
}

func runModbusServer(ctx context.Context, dev DeviceConfig, mem *InterfaceMemory, metricsWriter *UnifiedMetricsWriter) error {
	addr := fmt.Sprintf("tcp://%s:%d", dev.VirtualIP, modbusPort)
	handler := &modbusHandler{mem: mem, device: dev.Name, modbusUnitID: dev.ModbusUnitID, metricsWriter: metricsWriter}
	server, err := modbus.NewServer(&modbus.ServerConfiguration{URL: addr, Timeout: 30 * time.Second, MaxClients: 8}, handler)
	if err != nil {
		return fmt.Errorf("falha ao criar servidor modbus em %s: %w", addr, err)
	}
	if err := server.Start(); err != nil {
		return fmt.Errorf("falha ao iniciar servidor modbus em %s: %w", addr, err)
	}
	log.Printf("[%s][MODBUS] servidor escutando em %s unit=%d", dev.Name, addr, dev.ModbusUnitID)
	<-ctx.Done()
	if err := server.Stop(); err != nil {
		return fmt.Errorf("falha ao parar servidor modbus: %w", err)
	}
	return ctx.Err()
}

func (h *modbusHandler) HandleCoils(req *modbus.CoilsRequest) ([]bool, error) {
	return nil, modbus.ErrIllegalFunction
}

func (h *modbusHandler) HandleDiscreteInputs(req *modbus.DiscreteInputsRequest) ([]bool, error) {
	return nil, modbus.ErrIllegalFunction
}

func (h *modbusHandler) HandleHoldingRegisters(req *modbus.HoldingRegistersRequest) (res []uint16, err error) {
	start := time.Now()
	status := "ok"
	errText := ""
	defer func() {
		if h.metricsWriter != nil {
			_ = h.metricsWriter.Write(UnifiedMetric{
				Timestamp:    start,
				EventType:    "modbus_request",
				Device:       h.device,
				InterfaceName: virtualInterface,
				Direction:    "eth1_modbus_server",
				Protocol:     "Modbus/TCP",
				PeerAddr:     req.ClientAddr,
				Port:         modbusPort,
				PayloadBytes: int(req.Quantity) * 2,
				Duration:     time.Since(start),
				ClientAddr:   req.ClientAddr,
				UnitID:       req.UnitId,
				Area:         "holding",
				FunctionCode: modbusFunctionCodeHolding(req.IsWrite),
				IsWrite:      req.IsWrite,
				Addr:         req.Addr,
				Quantity:     req.Quantity,
				Status:       status,
				Error:        errText,
			})
		}
	}()

	if req.UnitId != h.modbusUnitID {
		status = "wrong_unit"
		errText = modbus.ErrIllegalFunction.Error()
		return nil, modbus.ErrIllegalFunction
	}
	current, _, _ := h.mem.ReadTx()
	regs := commandPayloadToHoldingRegs(current)
	end := int(req.Addr) + int(req.Quantity)
	if end > len(regs) {
		status = "bad_addr"
		errText = modbus.ErrIllegalDataAddress.Error()
		return nil, modbus.ErrIllegalDataAddress
	}
	if req.IsWrite {
		copy(regs[int(req.Addr):end], req.Args)
		updated, decErr := holdingRegsToCommandPayload(regs)
		if decErr != nil {
			status = "bad_value"
			errText = decErr.Error()
			return nil, modbus.ErrIllegalDataValue
		}
		h.mem.UpdateTx(updated)
		regs = commandPayloadToHoldingRegs(updated)
		status = "write_ok"
	}
	res = append(res, regs[int(req.Addr):end]...)
	return res, nil
}

func (h *modbusHandler) HandleInputRegisters(req *modbus.InputRegistersRequest) (res []uint16, err error) {
	start := time.Now()
	status := "ok"
	errText := ""
	defer func() {
		if h.metricsWriter != nil {
			_ = h.metricsWriter.Write(UnifiedMetric{
				Timestamp:    start,
				EventType:    "modbus_request",
				Device:       h.device,
				InterfaceName: virtualInterface,
				Direction:    "eth1_modbus_server",
				Protocol:     "Modbus/TCP",
				PeerAddr:     req.ClientAddr,
				Port:         modbusPort,
				PayloadBytes: int(req.Quantity) * 2,
				Duration:     time.Since(start),
				ClientAddr:   req.ClientAddr,
				UnitID:       req.UnitId,
				Area:         "input",
				FunctionCode: modbusFunctionCodeInput(),
				IsWrite:      false,
				Addr:         req.Addr,
				Quantity:     req.Quantity,
				Status:       status,
				Error:        errText,
			})
		}
	}()

	if req.UnitId != h.modbusUnitID {
		status = "wrong_unit"
		errText = modbus.ErrIllegalFunction.Error()
		return nil, modbus.ErrIllegalFunction
	}
	current, _, _, _, _, _ := h.mem.ReadRx()
	regs := payloadToInputRegs(current)
	end := int(req.Addr) + int(req.Quantity)
	if end > len(regs) {
		status = "bad_addr"
		errText = modbus.ErrIllegalDataAddress.Error()
		return nil, modbus.ErrIllegalDataAddress
	}
	res = append(res, regs[int(req.Addr):end]...)
	return res, nil
}

func runUDPReceiver(ctx context.Context, listenIP string, dev DeviceConfig, mem *InterfaceMemory, metricsWriter *UnifiedMetricsWriter) error {
	conn, err := listenUDPOnInterface(realInterface, listenIP, dev.TxPort)
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Printf("[%s][UDP-RX] escutando em %s:%d na rede física", dev.Name, listenIP, dev.TxPort)
	buf := make([]byte, udpBufferSize)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(udpReadTimeout))
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					continue
				}
			}
			return err
		}
		payload, err := decodeStatusPayload(buf[:n])
		if err != nil {
			log.Printf("[%s][UDP-RX] payload inválido de %s: %v", dev.Name, addr.String(), err)
			continue
		}
		mem.UpdateRx(payload, addr.String(), transportUDP)
		_, _, updAt, rxVer, _, rxProto := mem.ReadRx()
		if metricsWriter != nil {
			_ = metricsWriter.Write(UnifiedMetric{
				Timestamp:     time.Now(),
				EventType:     "physical_io",
				Device:        dev.Name,
				InterfaceName: realInterface,
				Direction:     "eth0_in_from_opal",
				Protocol:      rxProto,
				PeerAddr:      addr.String(),
				Port:          dev.TxPort,
				PayloadBytes:  n,
				Duration:      0,
				MemUpdateAt:   updAt,
				MemoryAge:     time.Since(updAt),
				RxVersion:     rxVer,
				Status:        "updated",
			})
		}
		log.Printf("[%s][UDP-RX] %s -> memória RX atualizada", dev.Name, addr.String())
	}
}

func runTCPReceiver(ctx context.Context, listenIP string, dev DeviceConfig, mem *InterfaceMemory, metricsWriter *UnifiedMetricsWriter) error {
	lc := net.ListenConfig{Control: controlBindToDevice(realInterface)}
	ln, err := lc.Listen(ctx, "tcp4", fmt.Sprintf("%s:%d", listenIP, dev.TxPort))
	if err != nil {
		return err
	}
	defer ln.Close()
	log.Printf("[%s][TCP-RX] escutando em %s:%d na rede física", dev.Name, listenIP, dev.TxPort)
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("[%s][TCP-RX] erro no accept: %v", dev.Name, err)
			}
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			_ = c.SetReadDeadline(time.Now().Add(tcpReadTimeout))
			buf := make([]byte, payloadSize())
			if _, err := io.ReadFull(c, buf); err != nil {
				log.Printf("[%s][TCP-RX] erro lendo payload de %s: %v", dev.Name, c.RemoteAddr().String(), err)
				return
			}
			payload, err := decodeStatusPayload(buf)
			if err != nil {
				log.Printf("[%s][TCP-RX] payload inválido de %s: %v", dev.Name, c.RemoteAddr().String(), err)
				return
			}
			mem.UpdateRx(payload, c.RemoteAddr().String(), transportTCP)
			_, _, updAt, rxVer, _, rxProto := mem.ReadRx()
			if metricsWriter != nil {
				_ = metricsWriter.Write(UnifiedMetric{
					Timestamp:     time.Now(),
					EventType:     "physical_io",
					Device:        dev.Name,
					InterfaceName: realInterface,
					Direction:     "eth0_in_from_opal",
					Protocol:      rxProto,
					PeerAddr:      c.RemoteAddr().String(),
					Port:          dev.TxPort,
					PayloadBytes:  len(buf),
					Duration:      0,
					MemUpdateAt:   updAt,
					MemoryAge:     time.Since(updAt),
					RxVersion:     rxVer,
					Status:        "updated",
				})
			}
			log.Printf("[%s][TCP-RX] %s -> memória RX atualizada", dev.Name, c.RemoteAddr().String())
		}(conn)
	}
}

func runPhysicalCommandSender(ctx context.Context, dev DeviceConfig, mem *InterfaceMemory, transport string, sendMode string, cycle time.Duration, metricsWriter *UnifiedMetricsWriter) error {
	ticker := time.NewTicker(cycle)
	defer ticker.Stop()
	var cycleIndex uint64
	var lastSuccessful CommandPayload
	var initialized bool

	log.Printf("[%s][CMD-TX] iniciado | cycle=%s -> %s:%d via %s | retries=%d",
		dev.Name, cycle.String(), opalRealIP, dev.RxPort, transport, maxCommandSendRetries)

	var udpConn *net.UDPConn
	var err error
	if transport == transportUDP || transport == transportBoth {
		udpConn, err = dialUDPOnInterface(realInterface, opalRealIP, dev.RxPort)
		if err != nil {
			return err
		}
		defer udpConn.Close()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			cycleIndex++
			payload, hasNew, version := mem.ReadTx()
			_, rxHas, rxTime, rxVer, lastSource, rxProtocol := mem.ReadRx()
			rxAge := time.Duration(0)
			rxFresh := false
			if rxHas {
				rxAge = time.Since(rxTime)
				rxFresh = rxAge <= staleTimeout
			}

			metric := CommandCycleMetric{
				Timestamp:         time.Now(),
				Device:            dev.Name,
				CycleIndex:        cycleIndex,
				Transport:         transport,
				PhysicalDirection: "eth0_out_to_opal",
				HasNewTX:          hasNew,
				TxVersion:         version,
				RxFresh:           rxFresh,
				RxAge:             rxAge,
				RxVersion:         rxVer,
				LastSource:        lastSource,
				RxProtocol:        rxProtocol,
				RxUpdatedAt:       rxTime,
				MemoryUpdateAge:   rxAge,
			}

			if !hasNew {
				metric.CompletionState = "idle"
				if metricsWriter != nil {
					_ = metricsWriter.Write(UnifiedMetric{
				Timestamp:         metric.Timestamp,
				EventType:         "command_cycle",
				Device:            metric.Device,
				InterfaceName:     realInterface,
				Direction:         metric.PhysicalDirection,
				Protocol:          metric.Transport,
				PayloadBytes:      metric.PayloadBytes,
				Duration:          metric.SendDuration,
				CycleIndex:        metric.CycleIndex,
				Transport:         metric.Transport,
				HasNewTX:          metric.HasNewTX,
				ShouldSend:        metric.ShouldSend,
				SendAttempted:     metric.SendAttempted,
				SendSuccess:       metric.SendSuccess,
				RetryAttempts:     metric.RetryAttempts,
				CompletionState:   metric.CompletionState,
				LastError:         metric.LastError,
				TxVersion:         metric.TxVersion,
				RxFresh:           metric.RxFresh,
				RxAge:             metric.RxAge,
				RxVersion:         metric.RxVersion,
				LastSource:        metric.LastSource,
				RxProtocol:        metric.RxProtocol,
				RxUpdatedAt:       metric.RxUpdatedAt,
				MemoryUpdateAge:   metric.MemoryUpdateAge,
				Status:            metric.CompletionState,
			})
				}
				continue
			}

			shouldSend := false
			switch sendMode {
			case sendModeAlways:
				shouldSend = true
			case sendModeOnChange:
				if !initialized || !reflect.DeepEqual(payload, lastSuccessful) {
					shouldSend = true
				}
			default:
				shouldSend = true
			}
			metric.ShouldSend = shouldSend

			if !shouldSend {
				metric.CompletionState = "unchanged"
				if metricsWriter != nil {
					_ = metricsWriter.Write(UnifiedMetric{
				Timestamp:         metric.Timestamp,
				EventType:         "command_cycle",
				Device:            metric.Device,
				InterfaceName:     realInterface,
				Direction:         metric.PhysicalDirection,
				Protocol:          metric.Transport,
				PayloadBytes:      metric.PayloadBytes,
				Duration:          metric.SendDuration,
				CycleIndex:        metric.CycleIndex,
				Transport:         metric.Transport,
				HasNewTX:          metric.HasNewTX,
				ShouldSend:        metric.ShouldSend,
				SendAttempted:     metric.SendAttempted,
				SendSuccess:       metric.SendSuccess,
				RetryAttempts:     metric.RetryAttempts,
				CompletionState:   metric.CompletionState,
				LastError:         metric.LastError,
				TxVersion:         metric.TxVersion,
				RxFresh:           metric.RxFresh,
				RxAge:             metric.RxAge,
				RxVersion:         metric.RxVersion,
				LastSource:        metric.LastSource,
				RxProtocol:        metric.RxProtocol,
				RxUpdatedAt:       metric.RxUpdatedAt,
				MemoryUpdateAge:   metric.MemoryUpdateAge,
				Status:            metric.CompletionState,
			})
				}
				continue
			}

			raw, err := encodeCommandPayload(payload)
			metric.SendAttempted = true
			if err != nil {
				log.Printf("[%s][CMD-TX] erro serializando: %v", dev.Name, err)
				metric.CompletionState = "serialize_error"
				metric.LastError = err.Error()
				if metricsWriter != nil {
					_ = metricsWriter.Write(UnifiedMetric{
				Timestamp:         metric.Timestamp,
				EventType:         "command_cycle",
				Device:            metric.Device,
				InterfaceName:     realInterface,
				Direction:         metric.PhysicalDirection,
				Protocol:          metric.Transport,
				PayloadBytes:      metric.PayloadBytes,
				Duration:          metric.SendDuration,
				CycleIndex:        metric.CycleIndex,
				Transport:         metric.Transport,
				HasNewTX:          metric.HasNewTX,
				ShouldSend:        metric.ShouldSend,
				SendAttempted:     metric.SendAttempted,
				SendSuccess:       metric.SendSuccess,
				RetryAttempts:     metric.RetryAttempts,
				CompletionState:   metric.CompletionState,
				LastError:         metric.LastError,
				TxVersion:         metric.TxVersion,
				RxFresh:           metric.RxFresh,
				RxAge:             metric.RxAge,
				RxVersion:         metric.RxVersion,
				LastSource:        metric.LastSource,
				RxProtocol:        metric.RxProtocol,
				RxUpdatedAt:       metric.RxUpdatedAt,
				MemoryUpdateAge:   metric.MemoryUpdateAge,
				Status:            metric.CompletionState,
			})
				}
				continue
			}

			metric.PayloadBytes = len(raw)
			sendStart := time.Now()
			sendOK := false
			var lastErr error
			attempts := 0

			tryUDPOnce := func() {
				attempts++
				if _, err := udpConn.Write(raw); err != nil {
					lastErr = err
					log.Printf("[%s][CMD-TX][UDP] tentativa %d/%d erro enviando para %s:%d: %v",
						dev.Name, attempts, 1, opalRealIP, dev.RxPort, err)
					return
				}
				sendOK = true
			}

			tryTCPRetries := func(maxRetries int) {
				for i := 1; i <= maxRetries; i++ {
					attempts++
					if err := sendTCPCommandPhysical(opalRealIP, dev.RxPort, raw); err != nil {
						lastErr = err
						log.Printf("[%s][CMD-TX][TCP] tentativa %d/%d erro enviando para %s:%d: %v",
							dev.Name, i, maxRetries, opalRealIP, dev.RxPort, err)

						if i < maxRetries {
							select {
							case <-ctx.Done():
								return
							case <-time.After(commandRetryDelay):
							}
						}
						continue
					}
					sendOK = true
					return
				}
			}

			switch transport {
			case transportUDP:
				tryUDPOnce()
			case transportTCP:
				tryTCPRetries(maxCommandSendRetries)
			case transportBoth:
				udpOK := false
				tcpOK := false

				// UDP é tentativa única
				if _, err := udpConn.Write(raw); err != nil {
					lastErr = err
					attempts++
					log.Printf("[%s][CMD-TX][UDP] tentativa %d/%d erro enviando para %s:%d: %v",
						dev.Name, attempts, attempts, opalRealIP, dev.RxPort, err)
				} else {
					attempts++
					udpOK = true
				}

				// TCP com retries
				for i := 1; i <= maxCommandSendRetries; i++ {
					attempts++
					if err := sendTCPCommandPhysical(opalRealIP, dev.RxPort, raw); err != nil {
						lastErr = err
						log.Printf("[%s][CMD-TX][TCP] tentativa %d/%d erro enviando para %s:%d: %v",
							dev.Name, i, maxCommandSendRetries, opalRealIP, dev.RxPort, err)
						if i < maxCommandSendRetries {
							select {
							case <-ctx.Done():
								return ctx.Err()
							case <-time.After(commandRetryDelay):
							}
						}
						continue
					}
					tcpOK = true
					break
				}

				sendOK = udpOK && tcpOK
			}

			metric.RetryAttempts = attempts
			metric.SendDuration = time.Since(sendStart)
			metric.SendSuccess = sendOK

			if sendOK {
				lastSuccessful = payload
				initialized = true
				mem.MarkTxSent(version)
				metric.CompletionState = "sent"
				log.Printf("[%s][CMD-TX] comando enviado com sucesso -> %s:%d via %s", dev.Name, opalRealIP, dev.RxPort, transport)
			} else {
				mem.MarkTxFailed(version)
				metric.CompletionState = "failed_exhausted"
				if lastErr != nil {
					metric.LastError = lastErr.Error()
				}
				log.Printf("[%s][CMD-TX] falha após %d tentativa(s); comando marcado como não concluído e aguardará próxima mudança",
					dev.Name, attempts)
			}

			if metricsWriter != nil {
				_ = metricsWriter.Write(UnifiedMetric{
					Timestamp:       metric.Timestamp,
					EventType:       "command_cycle",
					Device:          metric.Device,
					InterfaceName:   realInterface,
					Direction:       metric.PhysicalDirection,
					Protocol:        metric.Transport,
					PayloadBytes:    metric.PayloadBytes,
					Duration:        metric.SendDuration,
					CycleIndex:      metric.CycleIndex,
					Transport:       metric.Transport,
					HasNewTX:        metric.HasNewTX,
					ShouldSend:      metric.ShouldSend,
					SendAttempted:   metric.SendAttempted,
					SendSuccess:     metric.SendSuccess,
					RetryAttempts:   metric.RetryAttempts,
					CompletionState: metric.CompletionState,
					LastError:       metric.LastError,
					TxVersion:       metric.TxVersion,
					RxFresh:         metric.RxFresh,
					RxAge:           metric.RxAge,
					RxVersion:       metric.RxVersion,
					LastSource:      metric.LastSource,
					RxProtocol:      metric.RxProtocol,
					RxUpdatedAt:     metric.RxUpdatedAt,
					MemoryUpdateAge: metric.MemoryUpdateAge,
					Status:          metric.CompletionState,
				})
				if metric.SendAttempted {
					peer := fmt.Sprintf("%s:%d", opalRealIP, dev.RxPort)
					_ = metricsWriter.Write(UnifiedMetric{
						Timestamp:     metric.Timestamp,
						EventType:     "physical_io",
						Device:        dev.Name,
						InterfaceName: realInterface,
						Direction:     "eth0_out_to_opal",
						Protocol:      metric.Transport,
						PeerAddr:      peer,
						Port:          dev.RxPort,
						PayloadBytes:  metric.PayloadBytes,
						Duration:      metric.SendDuration,
						TxVersion:     metric.TxVersion,
						MemUpdateAt:   metric.RxUpdatedAt,
						MemoryAge:     metric.MemoryUpdateAge,
						Status:        metric.CompletionState,
						Error:         metric.LastError,
					})
				}
			}
		}
	}
}

func sendTCPCommandPhysical(destIP string, destPort int, payload []byte) error {
	dialer := net.Dialer{Timeout: tcpConnectTimeout, Control: controlBindToDevice(realInterface)}
	conn, err := dialer.Dial("tcp4", fmt.Sprintf("%s:%d", destIP, destPort))
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetWriteDeadline(time.Now().Add(tcpWriteTimeout))
	_, err = conn.Write(payload)
	return err
}

func payloadToInputRegs(p Payload) []uint16 {
	regs := make([]uint16, 44)
	regs[0], regs[1] = uint32ToRegsLowFirst(uint32(p.Status[0]))
	regs[2], regs[3] = uint32ToRegsLowFirst(uint32(p.Status[1]))
	idx := 4
	for i := 0; i < 20; i++ {
		regs[idx], regs[idx+1] = float32ToRegsLowFirst(p.Feedback[i])
		idx += 2
	}
	return regs
}

func commandPayloadToHoldingRegs(c CommandPayload) []uint16 {
	regs := make([]uint16, 44)
	regs[0], regs[1] = uint32ToRegsLowFirst(uint32(c.Com[0]))
	regs[2], regs[3] = uint32ToRegsLowFirst(uint32(c.Com[1]))
	idx := 4
	for i := 0; i < 20; i++ {
		regs[idx], regs[idx+1] = float32ToRegsLowFirst(c.Ref[i])
		idx += 2
	}
	return regs
}

func holdingRegsToCommandPayload(regs []uint16) (CommandPayload, error) {
	var c CommandPayload
	if len(regs) != 44 {
		return c, fmt.Errorf("quantidade inválida de holding regs: %d", len(regs))
	}
	c.Com[0] = int32(regsToUint32LowFirst(regs[0], regs[1]))
	c.Com[1] = int32(regsToUint32LowFirst(regs[2], regs[3]))
	idx := 4
	for i := 0; i < 20; i++ {
		c.Ref[i] = regsToFloat32LowFirst(regs[idx], regs[idx+1])
		idx += 2
	}
	return c, nil
}

func uint32ToRegsLowFirst(v uint32) (uint16, uint16) {
	return uint16(v & 0xffff), uint16((v >> 16) & 0xffff)
}

func regsToUint32LowFirst(low uint16, high uint16) uint32 {
	return uint32(low) | (uint32(high) << 16)
}

func float32ToRegsLowFirst(v float32) (uint16, uint16) {
	u := binary.LittleEndian.Uint32(mustFloat32Bytes(v))
	return uint32ToRegsLowFirst(u)
}

func regsToFloat32LowFirst(low uint16, high uint16) float32 {
	u := regsToUint32LowFirst(low, high)
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, u)
	var out float32
	_ = binary.Read(bytes.NewReader(b), binary.LittleEndian, &out)
	return out
}

func mustFloat32Bytes(f float32) []byte {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, f)
	return buf.Bytes()
}

func payloadSize() int { return (2 * 4) + (20 * 4) }

func decodeStatusPayload(b []byte) (Payload, error) {
	var p Payload
	if len(b) < payloadSize() {
		return p, fmt.Errorf("tamanho inválido: recebido=%d esperado=%d", len(b), payloadSize())
	}
	r := bytes.NewReader(b[:payloadSize()])
	for i := 0; i < 2; i++ {
		var u uint32
		if err := binary.Read(r, binary.LittleEndian, &u); err != nil {
			return p, err
		}
		p.Status[i] = int32(u)
	}
	for i := 0; i < 20; i++ {
		if err := binary.Read(r, binary.LittleEndian, &p.Feedback[i]); err != nil {
			return p, err
		}
	}
	return p, nil
}

func encodeCommandPayload(c CommandPayload) ([]byte, error) {
	buf := new(bytes.Buffer)
	for i := 0; i < 2; i++ {
		u := uint32(c.Com[i])
		if err := binary.Write(buf, binary.LittleEndian, u); err != nil {
			return nil, err
		}
	}
	for i := 0; i < 20; i++ {
		if err := binary.Write(buf, binary.LittleEndian, c.Ref[i]); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

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

func listenUDPOnInterface(ifName, listenIP string, port int) (*net.UDPConn, error) {
	lc := net.ListenConfig{Control: controlBindToDevice(ifName)}
	pc, err := lc.ListenPacket(context.Background(), "udp4", fmt.Sprintf("%s:%d", listenIP, port))
	if err != nil {
		return nil, err
	}
	udp, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close()
		return nil, fmt.Errorf("ListenPacket não retornou UDPConn")
	}
	return udp, nil
}

func dialUDPOnInterface(ifName, destIP string, port int) (*net.UDPConn, error) {
	dialer := net.Dialer{Control: controlBindToDevice(ifName)}
	c, err := dialer.Dial("udp4", fmt.Sprintf("%s:%d", destIP, port))
	if err != nil {
		return nil, err
	}
	udp, ok := c.(*net.UDPConn)
	if !ok {
		_ = c.Close()
		return nil, fmt.Errorf("Dial não retornou UDPConn")
	}
	return udp, nil
}
