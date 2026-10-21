package main

import (
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
	"sync"
	"syscall"
	"time"
)

const (
	realInterface    = "eth0"
	virtualInterface = "eth1"

	listenPort = 502

	tcpConnectTimeout = 3 * time.Second
	tcpReadTimeout    = 10 * time.Second
	tcpWriteTimeout   = 10 * time.Second
	tcpIdleTimeout    = 300 * time.Second

	enableRequestCSV = true
	requestCSVPath   = "./plc_scan_metrics.csv"
)

type DeviceRoute struct {
	ID        byte
	Name      string
	VirtualIP string
	Port      int
}

var routesByID = map[byte]DeviceRoute{
	0: {ID: 0, Name: "POI", VirtualIP: "192.168.1.30", Port: 502},
	1: {ID: 1, Name: "BESS", VirtualIP: "192.168.1.31", Port: 502},
	2: {ID: 2, Name: "GMG", VirtualIP: "192.168.1.32", Port: 502},
	3: {ID: 3, Name: "PV", VirtualIP: "192.168.1.33", Port: 502},
	4: {ID: 4, Name: "LD0", VirtualIP: "192.168.1.34", Port: 502},
	5: {ID: 5, Name: "LD1", VirtualIP: "192.168.1.35", Port: 502},
}

type RequestLogRecord struct {
	Timestamp     time.Time
	ClientAddr    string
	UnitID        byte
	DeviceName    string
	VirtualIP     string
	TransactionID uint16
	FunctionCode  byte
	ReqBytes      int
	RespBytes     int
	DurationMS    float64
	Status        string
	ErrorText     string
}

type RequestCSVWriter struct {
	enabled bool
	path    string
	mu      sync.Mutex
	file    *os.File
	writer  *csv.Writer
}

func NewRequestCSVWriter(enabled bool, path string) (*RequestCSVWriter, error) {
	w := &RequestCSVWriter{enabled: enabled, path: path}
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
		header := []string{
			"timestamp",
			"client_addr",
			"unit_id",
			"device_name",
			"virtual_ip",
			"transaction_id",
			"function_code",
			"req_bytes",
			"resp_bytes",
			"duration_ms",
			"status",
			"error",
		}
		if err := w.writer.Write(header); err != nil {
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

func (w *RequestCSVWriter) Write(rec RequestLogRecord) error {
	if !w.enabled {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	row := []string{
		rec.Timestamp.Format(time.RFC3339Nano),
		rec.ClientAddr,
		fmt.Sprintf("%d", rec.UnitID),
		rec.DeviceName,
		rec.VirtualIP,
		fmt.Sprintf("%d", rec.TransactionID),
		fmt.Sprintf("%d", rec.FunctionCode),
		fmt.Sprintf("%d", rec.ReqBytes),
		fmt.Sprintf("%d", rec.RespBytes),
		fmt.Sprintf("%.6f", rec.DurationMS),
		rec.Status,
		rec.ErrorText,
	}

	if err := w.writer.Write(row); err != nil {
		return err
	}
	w.writer.Flush()
	return w.writer.Error()
}

func (w *RequestCSVWriter) Close() error {
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

	csvWriter, err := NewRequestCSVWriter(enableRequestCSV, requestCSVPath)
	if err != nil {
		log.Fatalf("erro abrindo CSV de requisições: %v", err)
	}
	defer func() {
		if err := csvWriter.Close(); err != nil {
			log.Printf("erro fechando CSV de requisições: %v", err)
		}
	}()

	log.Printf("ems_router_cplx Modbus/TCP router iniciado | eth0=%s | eth1=%s | listen=%s:%d | csv=%t path=%s", realIP, virtualIP, realIP, listenPort, enableRequestCSV, requestCSVPath)
	log.Printf("mapeamento Unit ID -> device/IP virtual:")
	for id, route := range routesByID {
		log.Printf("  ID=%d -> %s (%s:%d)", id, route.Name, route.VirtualIP, route.Port)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runModbusIDRouter(ctx, realIP, csvWriter); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("erro no router: %v", err)
	}
}

func runModbusIDRouter(ctx context.Context, listenIP string, csvWriter *RequestCSVWriter) error {
	lc := net.ListenConfig{
		Control: controlBindToDevice(realInterface),
	}

	addr := fmt.Sprintf("%s:%d", listenIP, listenPort)
	ln, err := lc.Listen(ctx, "tcp4", addr)
	if err != nil {
		return fmt.Errorf("falha ao escutar em %s: %w", addr, err)
	}
	defer ln.Close()

	log.Printf("[ROUTER] escutando Modbus/TCP em %s pela %s", addr, realInterface)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("[ROUTER] erro no accept: %v", err)
			}
			continue
		}

		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			if err := handleClientConn(ctx, c, csvWriter); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[CLIENT %s] encerrado com erro: %v", c.RemoteAddr().String(), err)
			}
		}(conn)
	}
}

func handleClientConn(ctx context.Context, clientConn net.Conn, csvWriter *RequestCSVWriter) error {
	clientAddr := clientConn.RemoteAddr().String()
	log.Printf("[CLIENT %s] conexão recebida", clientAddr)

	backendConns := make(map[byte]net.Conn)
	defer func() {
		for _, c := range backendConns {
			_ = c.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_ = clientConn.SetReadDeadline(time.Now().Add(tcpIdleTimeout))

		adu, unitID, err := readModbusTCPADU(clientConn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("[CLIENT %s] conexão fechada pelo cliente", clientAddr)
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				log.Printf("[CLIENT %s] timeout de inatividade", clientAddr)
				return nil
			}
			return fmt.Errorf("erro lendo ADU Modbus/TCP: %w", err)
		}

		start := time.Now()
		tid, fc := parseADUMetadata(adu)

		route, ok := routesByID[unitID]
		if !ok {
			log.Printf("[CLIENT %s] Unit ID %d sem rota configurada", clientAddr, unitID)
			resp, werr := buildModbusExceptionResponse(adu, 0x0B)
			if werr != nil {
				return fmt.Errorf("falha ao montar exceção para unit id %d: %w", unitID, werr)
			}
			if _, err := clientConn.Write(resp); err != nil {
				return fmt.Errorf("falha ao responder exceção para unit id %d: %w", unitID, err)
			}
			_ = csvWriter.Write(RequestLogRecord{
				Timestamp:     start,
				ClientAddr:    clientAddr,
				UnitID:        unitID,
				DeviceName:    "",
				VirtualIP:     "",
				TransactionID: tid,
				FunctionCode:  fc,
				ReqBytes:      len(adu),
				RespBytes:     len(resp),
				DurationMS:    float64(time.Since(start).Microseconds()) / 1000.0,
				Status:        "no_route",
				ErrorText:     "unit_id_without_route",
			})
			continue
		}

		backendConn, ok := backendConns[unitID]
		if !ok {
			backendConn, err = dialBackend(route)
			if err != nil {
				log.Printf("[CLIENT %s] falha ao conectar backend %s (%s:%d): %v", clientAddr, route.Name, route.VirtualIP, route.Port, err)
				resp, werr := buildModbusExceptionResponse(adu, 0x0B)
				if werr != nil {
					return fmt.Errorf("falha ao montar exceção após erro de backend: %w", werr)
				}
				if _, err := clientConn.Write(resp); err != nil {
					return fmt.Errorf("falha ao responder exceção após erro de backend: %w", err)
				}
				_ = csvWriter.Write(RequestLogRecord{
					Timestamp:     start,
					ClientAddr:    clientAddr,
					UnitID:        unitID,
					DeviceName:    route.Name,
					VirtualIP:     route.VirtualIP,
					TransactionID: tid,
					FunctionCode:  fc,
					ReqBytes:      len(adu),
					RespBytes:     len(resp),
					DurationMS:    float64(time.Since(start).Microseconds()) / 1000.0,
					Status:        "backend_connect_error",
					ErrorText:     err.Error(),
				})
				continue
			}
			backendConns[unitID] = backendConn
			log.Printf("[CLIENT %s] backend conectado: ID=%d -> %s (%s:%d)", clientAddr, unitID, route.Name, route.VirtualIP, route.Port)
		}

		_ = backendConn.SetWriteDeadline(time.Now().Add(tcpWriteTimeout))
		if _, err := backendConn.Write(adu); err != nil {
			log.Printf("[CLIENT %s] erro escrevendo no backend %s: %v", clientAddr, route.Name, err)
			_ = backendConn.Close()
			delete(backendConns, unitID)
			resp, werr := buildModbusExceptionResponse(adu, 0x0B)
			if werr != nil {
				return fmt.Errorf("falha ao montar exceção após erro de escrita backend: %w", werr)
			}
			if _, err := clientConn.Write(resp); err != nil {
				return fmt.Errorf("falha ao responder exceção após erro de escrita backend: %w", err)
			}
			_ = csvWriter.Write(RequestLogRecord{
				Timestamp:     start,
				ClientAddr:    clientAddr,
				UnitID:        unitID,
				DeviceName:    route.Name,
				VirtualIP:     route.VirtualIP,
				TransactionID: tid,
				FunctionCode:  fc,
				ReqBytes:      len(adu),
				RespBytes:     len(resp),
				DurationMS:    float64(time.Since(start).Microseconds()) / 1000.0,
				Status:        "backend_write_error",
				ErrorText:     err.Error(),
			})
			continue
		}

		_ = backendConn.SetReadDeadline(time.Now().Add(tcpReadTimeout))
		respADU, respUnitID, err := readModbusTCPADU(backendConn)
		if err != nil {
			log.Printf("[CLIENT %s] erro lendo resposta do backend %s: %v", clientAddr, route.Name, err)
			_ = backendConn.Close()
			delete(backendConns, unitID)
			resp, werr := buildModbusExceptionResponse(adu, 0x0B)
			if werr != nil {
				return fmt.Errorf("falha ao montar exceção após erro de leitura backend: %w", werr)
			}
			if _, err := clientConn.Write(resp); err != nil {
				return fmt.Errorf("falha ao responder exceção após erro de leitura backend: %w", err)
			}
			_ = csvWriter.Write(RequestLogRecord{
				Timestamp:     start,
				ClientAddr:    clientAddr,
				UnitID:        unitID,
				DeviceName:    route.Name,
				VirtualIP:     route.VirtualIP,
				TransactionID: tid,
				FunctionCode:  fc,
				ReqBytes:      len(adu),
				RespBytes:     len(resp),
				DurationMS:    float64(time.Since(start).Microseconds()) / 1000.0,
				Status:        "backend_read_error",
				ErrorText:     err.Error(),
			})
			continue
		}

		if respUnitID != unitID {
			log.Printf("[CLIENT %s] resposta com Unit ID divergente: req=%d resp=%d backend=%s", clientAddr, unitID, respUnitID, route.Name)
		}

		_ = clientConn.SetWriteDeadline(time.Now().Add(tcpWriteTimeout))
		if _, err := clientConn.Write(respADU); err != nil {
			return fmt.Errorf("erro escrevendo resposta ao cliente: %w", err)
		}

		durationMS := float64(time.Since(start).Microseconds()) / 1000.0
		log.Printf("[CLIENT %s] roteado ID=%d -> %s (%s) | req_bytes=%d resp_bytes=%d | duration=%.3f ms",
			clientAddr, unitID, route.Name, route.VirtualIP, len(adu), len(respADU), durationMS)

		_ = csvWriter.Write(RequestLogRecord{
			Timestamp:     start,
			ClientAddr:    clientAddr,
			UnitID:        unitID,
			DeviceName:    route.Name,
			VirtualIP:     route.VirtualIP,
			TransactionID: tid,
			FunctionCode:  fc,
			ReqBytes:      len(adu),
			RespBytes:     len(respADU),
			DurationMS:    durationMS,
			Status:        "ok",
			ErrorText:     "",
		})
	}
}

func dialBackend(route DeviceRoute) (net.Conn, error) {
	dialer := net.Dialer{
		Timeout: tcpConnectTimeout,
		Control: controlBindToDevice(virtualInterface),
	}
	return dialer.Dial("tcp4", fmt.Sprintf("%s:%d", route.VirtualIP, route.Port))
}

// MBAP = 7 bytes: TID(2) PID(2) LEN(2) UID(1)
// LEN inclui UID + PDU. Total ADU = 6 + LEN bytes.
func readModbusTCPADU(r io.Reader) ([]byte, byte, error) {
	header := make([]byte, 7)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, 0, err
	}

	length := binary.BigEndian.Uint16(header[4:6])
	if length == 0 {
		return nil, 0, fmt.Errorf("MBAP length inválido: 0")
	}
	if length < 2 {
		return nil, 0, fmt.Errorf("MBAP length inválido: %d", length)
	}

	pduLen := int(length) - 1
	pdu := make([]byte, pduLen)
	if _, err := io.ReadFull(r, pdu); err != nil {
		return nil, 0, err
	}

	adu := append(header, pdu...)
	unitID := header[6]
	return adu, unitID, nil
}

func parseADUMetadata(adu []byte) (uint16, byte) {
	var tid uint16
	var fc byte
	if len(adu) >= 2 {
		tid = binary.BigEndian.Uint16(adu[0:2])
	}
	if len(adu) >= 8 {
		fc = adu[7]
	}
	return tid, fc
}

func buildModbusExceptionResponse(reqADU []byte, exceptionCode byte) ([]byte, error) {
	if len(reqADU) < 8 {
		return nil, fmt.Errorf("requisição curta demais para montar exceção")
	}

	tid := reqADU[0:2]
	pid := reqADU[2:4]
	unitID := reqADU[6]
	functionCode := reqADU[7]

	resp := make([]byte, 9)
	copy(resp[0:2], tid)
	copy(resp[2:4], pid)
	binary.BigEndian.PutUint16(resp[4:6], 3)
	resp[6] = unitID
	resp[7] = functionCode | 0x80
	resp[8] = exceptionCode
	return resp, nil
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
