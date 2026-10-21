## Estágio 1: Build
FROM golang:1.24-alpine AS builder

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia os arquivos de dependência e baixa
COPY go.mod ./
RUN go mod download

# Copia o código-fonte
COPY . .

# Compila o binário. O CGO_ENABLED=0 garante um binário estático
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o ems_router_cplx .

# Estágio 2: Produção (Imagem Alpine Final)
FROM alpine:latest

# baixar o tcpdump para capturar pacotes
RUN apk add --no-cache tcpdump

# Define diretório de trabalho
WORKDIR /app

# Copia o binário do estágio de build
COPY --from=builder /app/ems_router_cplx .

# Executa o binário
CMD ["/bin/sh"]
#ENTRYPOINT [ "/app" ]