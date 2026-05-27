# Plataforma de Monitoramento de Laboratório — UnB
**CIC0124 - Redes de Computadores | Projeto 1**

---

## Visão Geral

Sistema cliente-servidor para monitoramento em tempo real de sensores de laboratório
(temperatura, umidade, CO₂ e CPU), com painel web, alertas automáticos e métricas de rede.

---

## Protocolo de Aplicação (JSON sobre TCP)

Todas as mensagens são objetos JSON terminados por `\n` (newline), trafegando sobre TCP.

### Tipos de mensagem - Cliente -> Servidor

| Tipo        | Campos obrigatórios                                   | Descrição                          |
|-------------|-------------------------------------------------------|------------------------------------|
| `cadastro`  | `nome`, `email`, `senha`                              | Registrar novo usuário             |
| `login`     | `email`, `senha`                                      | Autenticar usuário existente       |
| `registro`  | `sensor_id`, `lab`, `tipo_sensor`                     | Registrar sensor na sessão TCP     |
| `leitura`   | `valor`, `unidade`, `t_envio` (unix timestamp)        | Enviar leitura de sensor           |
| `ping`      | `t_envio` (unix timestamp)                            | Medir RTT puro                     |
| `status`    | `target` (sensor_id)                                  | Consultar estado de outro sensor   |
| `metricas`  | -                                                     | Solicitar métricas de RTT da sessão|
| `quit`      | -                                                     | Encerrar conexão graciosamente     |

### Tipos de mensagem - Servidor -> Cliente

| Tipo        | Campos                                                | Descrição                          |
|-------------|-------------------------------------------------------|------------------------------------|
| ACK ok      | `status:"ok"`, `rtt_ms`, `alerta`, `timestamp`        | Confirmação de leitura             |
| `pong`      | `tipo:"pong"`, `rtt_ms`, `t_server`                   | Resposta ao ping                   |
| `alerta`    | `tipo:"alerta"`, `sensor_id`, `valor`, `mensagem`     | Broadcast de alerta para o lab     |
| `status`    | `tipo:"status"`, `sensor_id`, `evento`                | Notificação de conexão/desconexão  |
| erro        | `status:"erro"`, `msg`                                | Mensagem de erro                   |

---

## Limiares de Alerta

| Sensor      | Mínimo | Máximo | Unidade |
|-------------|--------|--------|---------|
| temperatura | 10.0   | 30.0   | °C      |
| umidade     | 30.0   | 80.0   | %       |
| co2         | 0.0    | 1000.0 | ppm     |
| cpu         | 0.0    | 90.0   | %       |

---

## Endpoints HTTP (Painel Web)

| Método | Rota             | Descrição                                      |
|--------|------------------|------------------------------------------------|
| GET    | `/painel`        | Painel HTML com atualização automática (5s)    |
| GET    | `/api/sensores`  | Sensores online (JSON)                         |
| GET    | `/api/leituras`  | Histórico de leituras (`?lab=&tipo=&limit=`)   |
| GET    | `/api/alertas`   | Alertas recentes (`?limit=`)                   |
| GET    | `/api/metricas`  | Métricas agregadas (RTT mín/méd/máx)           |
| POST   | `/api/cadastro`  | Criar usuário via HTTP                         |
| POST   | `/api/login`     | Autenticar via HTTP                            |

---

## Como Executar

### Pré-requisitos

```bash
pip install PySimpleGUI
```

### 1. Iniciar o servidor

```bash
python server-monitoramento-lab.py
```

O servidor abre:
- TCP em `127.0.0.1:9000` (sensores)
- HTTP em `http://127.0.0.1:8080/painel` (painel web)

### 2. Iniciar o(s) cliente(s)

```bash
python client-monitoramento-lab.py
```

Cada instância representa um sensor. Podem-se abrir múltiplos clientes
simultâneos para simular vários sensores no mesmo laboratório.

---

## Arquivos Gerados em Execução

| Arquivo             | Conteúdo                                      |
|---------------------|-----------------------------------------------|
| `monitoramento.db`  | Banco SQLite com usuários, leituras e alertas |
| `servidor.log`      | Log do servidor (conexões, leituras, alertas) |
| `cliente.log`       | Log do cliente (envios, RTT, erros)           |
