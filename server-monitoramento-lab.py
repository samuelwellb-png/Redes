"""
=============================================
Servidor TCP de Monitoramento de Laboratório
CIC0124 - Redes de Computadores — UnB
=============================================
"""

import socket
import json
import time
import sqlite3
import logging
import threading
import http.server
import urllib.parse
from threading import Thread, Lock
from datetime import datetime

# ── Configurações ───────────────────────────────────────────────
HOST       = '127.0.0.1'
PORT_TCP   = 9000   # porta para sensores (TCP sockets)
PORT_HTTP  = 8080   # porta para painel web (HTTP)
DB_PATH    = 'monitoramento.db'
LOG_PATH   = 'servidor.log'

# ── Limiares de alerta ──────────────────────────────────────────
LIMITES = {
    "temperatura": (10.0, 30.0),
    "umidade":     (30.0, 80.0),
    "co2":         (0.0, 1000.0),
    "cpu":         (0.0, 90.0),
}

# ── Estado global (protegido por lock) ─────────────────────────
sensores     = {}   # sensor_id → {conn, addr, tipo, lab, ultimo_valor, rtts}
laboratorios = {}   # lab_id    → [sensor_id, ...]
lock         = Lock()

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════
#  BANCO DE DADOS (Persistência)
# ════════════════════════════════════════════════════════════════

def init_db():
    """Cria as tabelas caso não existam."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            nome     TEXT NOT NULL,
            email    TEXT NOT NULL UNIQUE,
            senha    TEXT NOT NULL,
            criado   TEXT DEFAULT (datetime('now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS leituras (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id  TEXT NOT NULL,
            lab        TEXT NOT NULL,
            tipo       TEXT NOT NULL,
            valor      REAL NOT NULL,
            unidade    TEXT,
            rtt_ms     REAL,
            timestamp  TEXT DEFAULT (datetime('now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS alertas (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id  TEXT NOT NULL,
            lab        TEXT NOT NULL,
            mensagem   TEXT NOT NULL,
            timestamp  TEXT DEFAULT (datetime('now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS logs_rede (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id  TEXT,
            evento     TEXT,
            rtt_ms     REAL,
            bytes_rx   INTEGER,
            timestamp  TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


def db_salvar_leitura(sensor_id, lab, tipo, valor, unidade, rtt_ms):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO leituras (sensor_id, lab, tipo, valor, unidade, rtt_ms) VALUES (?,?,?,?,?,?)",
        (sensor_id, lab, tipo, valor, unidade, rtt_ms)
    )
    conn.commit()
    conn.close()


def db_salvar_alerta(sensor_id, lab, mensagem):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO alertas (sensor_id, lab, mensagem) VALUES (?,?,?)",
        (sensor_id, lab, mensagem)
    )
    conn.commit()
    conn.close()


def db_log_rede(sensor_id, evento, rtt_ms=None, bytes_rx=None):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO logs_rede (sensor_id, evento, rtt_ms, bytes_rx) VALUES (?,?,?,?)",
        (sensor_id, evento, rtt_ms, bytes_rx)
    )
    conn.commit()
    conn.close()


def db_buscar_leituras(lab=None, tipo=None, limite=50):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    if lab and tipo:
        rows = conn.execute(
            "SELECT * FROM leituras WHERE lab=? AND tipo=? ORDER BY id DESC LIMIT ?",
            (lab, tipo, limite)
        ).fetchall()
    elif lab:
        rows = conn.execute(
            "SELECT * FROM leituras WHERE lab=? ORDER BY id DESC LIMIT ?",
            (lab, limite)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM leituras ORDER BY id DESC LIMIT ?", (limite,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def db_buscar_alertas(limite=20):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM alertas ORDER BY id DESC LIMIT ?", (limite,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def db_registrar_usuario(nome, email, senha):
    """Retorna True se criou, False se email já existe."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT INTO usuarios (nome, email, senha) VALUES (?,?,?)",
            (nome, email, senha)
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False


def db_autenticar(email, senha):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT nome FROM usuarios WHERE email=? AND senha=?", (email, senha)
    ).fetchone()
    conn.close()
    return row[0] if row else None


# ════════════════════════════════════════════════════════════════
#  PROTOCOLO TCP — Tratamento de cada sensor
# ════════════════════════════════════════════════════════════════

def calcular_rtt(t_envio, t_recebido):
    return round((t_recebido - t_envio) * 1000, 3)


def broadcastLab(msg, remetente, lab):
    """Envia JSON para todos os sensores do lab, exceto o remetente."""
    payload = (json.dumps(msg) + "\n").encode()
    with lock:
        membros = laboratorios.get(lab, []).copy()
    for sid in membros:
        if sid != remetente:
            try:
                with lock:
                    c = sensores[sid]["conn"]
                c.send(payload)
            except Exception:
                pass


def handleSensor(conn, addr):
    """
    Thread dedicada a um sensor.

    Protocolo (JSON sobre TCP, delimitado por \\n):
      → REGISTRO  {"tipo":"registro", "sensor_id":"s1", "lab":"lab-1", "tipo_sensor":"temperatura"}
      ← ACK       {"status":"ok", "msg":"registrado"}

      → LEITURA   {"tipo":"leitura", "valor":25.3, "unidade":"°C", "t_envio":<unix_ts>}
      ← ACK       {"status":"ok", "rtt_ms":12.4, "alerta":null, "timestamp":"..."}

      → PING      {"tipo":"ping", "t_envio":<unix_ts>}
      ← PONG      {"tipo":"pong", "rtt_ms":5.1}

      → STATUS    {"tipo":"status", "target":"s2"}
      ← INFO      {"status":"ok", "sensor":{...}}

      → CADASTRO  {"tipo":"cadastro", "nome":"...", "email":"...", "senha":"..."}
      ← ACK       {"status":"ok"|"erro", "msg":"..."}

      → LOGIN     {"tipo":"login", "email":"...", "senha":"..."}
      ← ACK       {"status":"ok", "nome":"..."}  |  {"status":"erro"}

      → QUIT      {"tipo":"quit"}
    """
    sensor_id = None
    lab       = None
    tipo      = None
    buffer    = ""

    def recv_json():
        """Recebe bytes até completar um JSON terminado por \\n."""
        nonlocal buffer
        while "\n" not in buffer:
            chunk = conn.recv(4096).decode('utf-8', errors='replace')
            if not chunk:
                raise ConnectionResetError("conexão encerrada")
            buffer += chunk
        line, buffer = buffer.split("\n", 1)
        return json.loads(line)

    def send_json(obj):
        conn.send((json.dumps(obj, ensure_ascii=False) + "\n").encode())

    try:
        # ── Primeiro pacote: registro ou autenticação ──
        msg = recv_json()

        if msg.get("tipo") == "cadastro":
            ok = db_registrar_usuario(msg["nome"], msg["email"], msg["senha"])
            send_json({"status": "ok" if ok else "erro",
                       "msg": "usuário criado" if ok else "e-mail já cadastrado"})
            conn.close()
            return

        if msg.get("tipo") == "login":
            nome = db_autenticar(msg["email"], msg["senha"])
            send_json({"status": "ok", "nome": nome} if nome
                      else {"status": "erro", "msg": "credenciais inválidas"})
            conn.close()
            return

        if msg.get("tipo") != "registro":
            send_json({"status": "erro", "msg": "primeiro pacote deve ser 'registro'"})
            conn.close()
            return

        sensor_id = msg["sensor_id"]
        lab       = msg.get("lab", "lab-geral")
        tipo      = msg.get("tipo_sensor", "desconhecido")

    except Exception as e:
        try:
            conn.send((json.dumps({"status": "erro", "msg": str(e)}) + "\n").encode())
        except Exception:
            pass
        conn.close()
        return

    # ── Registra sensor ──
    with lock:
        sensores[sensor_id] = {
            "conn": conn, "addr": str(addr),
            "tipo": tipo, "lab": lab,
            "ultimo_valor": None, "rtts": [],
            "conectado_em": datetime.now().isoformat(),
            "total_leituras": 0,
        }
        laboratorios.setdefault(lab, [])
        if sensor_id not in laboratorios[lab]:
            laboratorios[lab].append(sensor_id)

    log.info(f"CONECTADO {sensor_id} ({tipo}) lab='{lab}' addr={addr}")
    db_log_rede(sensor_id, "conectado")
    send_json({"status": "ok", "msg": f"sensor {sensor_id} registrado"})
    broadcastLab({"tipo": "status", "sensor_id": sensor_id, "evento": "conectado"},
                 remetente=sensor_id, lab=lab)

    # ── Loop principal ──
    while True:
        try:
            msg = recv_json()
            t_recebido = time.time()
            bytes_rx   = len(json.dumps(msg).encode())

        except (json.JSONDecodeError, KeyError) as e:
            send_json({"status": "erro", "msg": f"JSON inválido: {e}"})
            continue
        except Exception:
            break   # conexão perdida

        tipo_msg = msg.get("tipo")

        # ── LEITURA ──
        if tipo_msg == "leitura":
            try:
                valor   = float(msg["valor"])
                unidade = msg.get("unidade", "")
                t_envio = float(msg.get("t_envio", t_recebido))
                rtt_ms  = calcular_rtt(t_envio, t_recebido)

                with lock:
                    sensores[sensor_id]["ultimo_valor"]   = valor
                    sensores[sensor_id]["rtts"].append(rtt_ms)
                    sensores[sensor_id]["total_leituras"] += 1

                db_salvar_leitura(sensor_id, lab, tipo, valor, unidade, rtt_ms)
                db_log_rede(sensor_id, "leitura", rtt_ms, bytes_rx)
                log.info(f"LEITURA {sensor_id}: {valor}{unidade} RTT={rtt_ms}ms")

                # Verifica alerta
                alerta = None
                if tipo in LIMITES:
                    vmin, vmax = LIMITES[tipo]
                    if not (vmin <= valor <= vmax):
                        alerta = (f"ALERTA: {sensor_id}={valor}{unidade} "
                                  f"fora de [{vmin},{vmax}]")
                        log.warning(alerta)
                        db_salvar_alerta(sensor_id, lab, alerta)
                        broadcastLab(
                            {"tipo": "alerta", "sensor_id": sensor_id,
                             "valor": valor, "mensagem": alerta},
                            remetente=sensor_id, lab=lab
                        )

                send_json({
                    "status":    "ok",
                    "rtt_ms":    rtt_ms,
                    "alerta":    alerta,
                    "timestamp": datetime.now().isoformat(),
                })

            except (ValueError, KeyError) as e:
                send_json({"status": "erro", "msg": str(e)})

        # ── PING (medição de RTT pura) ──
        elif tipo_msg == "ping":
            t_envio = float(msg.get("t_envio", t_recebido))
            rtt_ms  = calcular_rtt(t_envio, t_recebido)
            db_log_rede(sensor_id, "ping", rtt_ms)
            send_json({"tipo": "pong", "rtt_ms": rtt_ms,
                       "t_server": t_recebido})

        # ── STATUS de outro sensor ──
        elif tipo_msg == "status":
            target = msg.get("target")
            with lock:
                if target in sensores:
                    info = {k: v for k, v in sensores[target].items()
                            if k not in ("conn",)}
                    send_json({"status": "ok", "sensor": info})
                else:
                    send_json({"status": "erro",
                               "msg": f"sensor '{target}' não encontrado"})

        # ── MÉTRICAS (throughput médio) ──
        elif tipo_msg == "metricas":
            with lock:
                rtts = sensores[sensor_id]["rtts"]
            if rtts:
                send_json({
                    "status":      "ok",
                    "rtt_min_ms":  round(min(rtts), 3),
                    "rtt_max_ms":  round(max(rtts), 3),
                    "rtt_avg_ms":  round(sum(rtts) / len(rtts), 3),
                    "amostras":    len(rtts),
                })
            else:
                send_json({"status": "ok", "msg": "sem amostras ainda"})

        # ── QUIT ──
        elif tipo_msg == "quit":
            log.info(f"DESCONEXÃO voluntária: {sensor_id}")
            break

        else:
            send_json({"status": "erro", "msg": f"tipo desconhecido: {tipo_msg}"})

    # ── Cleanup ──
    with lock:
        sensores.pop(sensor_id, None)
        if lab in laboratorios and sensor_id in laboratorios[lab]:
            laboratorios[lab].remove(sensor_id)

    broadcastLab({"tipo": "status", "sensor_id": sensor_id, "evento": "desconectado"},
                 remetente=sensor_id, lab=lab)
    db_log_rede(sensor_id, "desconectado")
    log.info(f"DESCONECTADO {sensor_id}")
    conn.close()


# ════════════════════════════════════════════════════════════════
#  SERVIDOR HTTP — Painel Web em Tempo Real
# ════════════════════════════════════════════════════════════════

HTML_PAINEL = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="5">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:ital,wght@0,100..700;1,100..700&display=swap" rel="stylesheet">
<title>Monitoramento de Laboratório</title>
<style>
  body {{font-family:'IBM Plex Sans'; background:#1a1a2e; color:#eee; margin:0; padding:20px;}}
  h1   {{color:#e94560; text-align:center;}}
  h2   {{color:#ffff; background:#16213e; padding:8px; border-radius:4px;}}
  table{{width:100%; border-collapse:collapse; margin-bottom:20px;}}
  th   {{background:#0f3460; padding:8px;}}
  td   {{padding:6px; border-bottom:1px solid #333; text-align:center;}}
  tr:hover {{background:#16213e;}}
  .alerta {{color:#e94560; font-weight:bold;}}
  .ok     {{color:#4caf50;}}
  .badge  {{display:inline-block; padding:2px 8px; border-radius:10px;
            background:#0f3460; font-size:0.85em;}}
</style>
</head>
<body>
<h1>Monitoramento de Laboratório - UnB</h1>
<p style="text-align:center;color:#aaa">Atualiza a cada 5 segundos · {hora}</p>
<h2>Sensores Conectados</h2>
{tabela_sensores}
<h2>Últimas Leituras</h2>
{tabela_leituras}
<h2>Últimos Alertas</h2>
{tabela_alertas}
</body>
</html>"""


def gerar_html():
    hora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    # sensores online
    with lock:
        sens_copy = {k: {**v} for k, v in sensores.items()}

    if sens_copy:
        linhas = ""
        for sid, s in sens_copy.items():
            rtts = s.get("rtts", [])
            rtt_avg = round(sum(rtts) / len(rtts), 2) if rtts else "-"
            linhas += (f"<tr><td>{sid}</td><td>{s['tipo']}</td><td>{s['lab']}</td>"
                       f"<td>{s.get('ultimo_valor','—')}</td>"
                       f"<td>{s.get('total_leituras',0)}</td>"
                       f"<td>{rtt_avg} ms</td></tr>")
        tabela_s = (f"<table><tr><th>ID</th><th>Tipo</th><th>Lab</th>"
                    f"<th>Último Valor</th><th>Leituras</th><th>RTT Médio</th></tr>"
                    f"{linhas}</table>")
    else:
        tabela_s = "<p style='color:#aaa'>Nenhum sensor conectado.</p>"

    # leituras recentes
    leituras = db_buscar_leituras(limite=20)
    if leituras:
        linhas = ""
        for l in leituras:
            linhas += (f"<tr><td>{l['sensor_id']}</td><td>{l['lab']}</td>"
                       f"<td>{l['tipo']}</td><td>{l['valor']} {l['unidade'] or ''}</td>"
                       f"<td>{l['rtt_ms']} ms</td><td>{l['timestamp']}</td></tr>")
        tabela_l = (f"<table><tr><th>Sensor</th><th>Lab</th><th>Tipo</th>"
                    f"<th>Valor</th><th>RTT</th><th>Timestamp</th></tr>"
                    f"{linhas}</table>")
    else:
        tabela_l = "<p style='color:#aaa'>Sem leituras registradas.</p>"

    # alertas
    alertas = db_buscar_alertas(limite=10)
    if alertas:
        linhas = ""
        for a in alertas:
            linhas += (f"<tr><td class='alerta'>{a['sensor_id']}</td>"
                       f"<td>{a['lab']}</td><td>{a['mensagem']}</td>"
                       f"<td>{a['timestamp']}</td></tr>")
        tabela_a = (f"<table><tr><th>Sensor</th><th>Lab</th>"
                    f"<th>Mensagem</th><th>Timestamp</th></tr>"
                    f"{linhas}</table>")
    else:
        tabela_a = "<p class='ok'>✅ Nenhum alerta registrado.</p>"

    return HTML_PAINEL.format(
        hora=hora,
        tabela_sensores=tabela_s,
        tabela_leituras=tabela_l,
        tabela_alertas=tabela_a,
    )


class PainelHandler(http.server.BaseHTTPRequestHandler):
    """Handler HTTP simples para o painel web e API REST."""

    def log_message(self, fmt, *args):
        log.info(f"HTTP {self.address_string()} {fmt % args}")

    def send_json_response(self, data, code=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        params = dict(urllib.parse.parse_qsl(parsed.query))

        # ── Painel HTML ──
        if path in ("/", "/painel"):
            html = gerar_html().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(html))
            self.end_headers()
            self.wfile.write(html)

        # ── API: sensores online ──
        elif path == "/api/sensores":
            with lock:
                dados = {
                    sid: {k: v for k, v in s.items() if k != "conn"}
                    for sid, s in sensores.items()
                }
            self.send_json_response(dados)

        # ── API: leituras ──
        elif path == "/api/leituras":
            lab   = params.get("lab")
            tipo  = params.get("tipo")
            limit = int(params.get("limit", 50))
            self.send_json_response(db_buscar_leituras(lab, tipo, limit))

        # ── API: alertas ──
        elif path == "/api/alertas":
            self.send_json_response(db_buscar_alertas(int(params.get("limit", 20))))

        # ── API: métricas agregadas ──
        elif path == "/api/metricas":
            conn_db = sqlite3.connect(DB_PATH)
            row = conn_db.execute(
                "SELECT COUNT(*), AVG(rtt_ms), MIN(rtt_ms), MAX(rtt_ms) FROM leituras"
            ).fetchone()
            conn_db.close()
            self.send_json_response({
                "total_leituras": row[0],
                "rtt_avg_ms":     round(row[1], 3) if row[1] else None,
                "rtt_min_ms":     round(row[2], 3) if row[2] else None,
                "rtt_max_ms":     round(row[3], 3) if row[3] else None,
            })

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length)
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            self.send_json_response({"status": "erro", "msg": "JSON inválido"}, 400)
            return

        # ── Cadastro via HTTP ──
        if self.path == "/api/cadastro":
            ok = db_registrar_usuario(data["nome"], data["email"], data["senha"])
            self.send_json_response(
                {"status": "ok"} if ok
                else {"status": "erro", "msg": "e-mail já cadastrado"}, 200 if ok else 409
            )

        # ── Login via HTTP ──
        elif self.path == "/api/login":
            nome = db_autenticar(data["email"], data["senha"])
            self.send_json_response(
                {"status": "ok", "nome": nome} if nome
                else {"status": "erro", "msg": "credenciais inválidas"}, 200 if nome else 401
            )

        else:
            self.send_json_response({"status": "erro", "msg": "rota não encontrada"}, 404)


def iniciar_http():
    httpd = http.server.HTTPServer((HOST, PORT_HTTP), PainelHandler)
    log.info(f"Painel HTTP em http://{HOST}:{PORT_HTTP}/painel")
    httpd.serve_forever()


# ════════════════════════════════════════════════════════════════
#  PONTO DE ENTRADA
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    init_db()

    # Thread HTTP (painel web)
    Thread(target=iniciar_http, daemon=True).start()

    # Socket TCP principal
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((HOST, PORT_TCP))
    server_sock.listen()
    log.info(f"Servidor TCP aguardando em {HOST}:{PORT_TCP}")
    log.info(f"Painel web: http://{HOST}:{PORT_HTTP}/painel")

    try:
        while True:
            conn, addr = server_sock.accept()
            Thread(target=handleSensor, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        log.info("Servidor encerrado pelo usuário.")
    finally:
        server_sock.close()
