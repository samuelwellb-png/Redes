"""
============================================
Cliente TCP de Monitoramento de Laboratório
CIC0124 - Redes de Computadores — UnB
============================================
"""

import socket
import json
import time
import queue
import random
import logging
import threading
from threading import Thread
from datetime import datetime

import PySimpleGUI as sg
import webbrowser

# ── Tema ────────────────────────────────────────────────────────
sg.theme('DarkBlue14')

# ── Rede ────────────────────────────────────────────────────────
HOST     = '127.0.0.1'
PORT_TCP = 9000

# ── Domínio ─────────────────────────────────────────────────────
LIMITES = {
    "temperatura": (10.0, 30.0),
    "umidade":     (30.0, 80.0),
    "co2":         (0.0, 1000.0),
    "cpu":         (0.0, 90.0),
}
UNIDADES = {
    "temperatura": "°C",
    "umidade":     "%",
    "co2":         "ppm",
    "cpu":         "%",
}

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("cliente.log", encoding="utf-8"),
              logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ── Fontes GUI ──────────────────────────────────────────────────
FONT_TITULO = ("Helvetica", 14, "bold")
FONT_NORMAL = ("Helvetica", 11)
FONT_MONO   = ("Courier New", 10)


# ════════════════════════════════════════════════════════════════
#  CLASSE DE CONEXÃO — toda I/O de rede fica aqui
# ════════════════════════════════════════════════════════════════

class ConexaoSensor:
    """
    Encapsula o socket TCP e uma thread de leitura dedicada.

    A thread de leitura classifica cada mensagem recebida:
      - mensagens com "status" (ACK de leitura/ping/etc.) → fila_respostas
      - mensagens assíncronas ("alerta", "status" de evento) → fila_eventos

    A GUI nunca chama recv() diretamente; só chama send() e get_resposta().
    """

    def __init__(self):
        self.sock          = None
        self._buf          = ""
        self._lock_send    = threading.Lock()
        self.fila_respostas = queue.Queue()   # ACKs aguardados pela GUI
        self.fila_eventos  = queue.Queue()    # alertas/status assíncronos
        self._ativa        = False
        self._thread       = None

        # métricas
        self.bytes_enviados   = 0
        self.bytes_recebidos  = 0
        self.rtts             = []
        self.total_leituras   = 0

    # ── Baixo nível ─────────────────────────────────────────────

    def _send_raw(self, obj):
        payload = (json.dumps(obj, ensure_ascii=False) + "\n").encode()
        with self._lock_send:
            self.sock.sendall(payload)
        self.bytes_enviados += len(payload)

    def _recv_linha(self):
        """Lê até '\\n' usando buffer interno. Chamada SOMENTE pela thread de leitura."""
        while "\n" not in self._buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionResetError("servidor encerrou a conexão")
            self._buf += chunk.decode("utf-8", errors="replace")
        linha, self._buf = self._buf.split("\n", 1)
        self.bytes_recebidos += len(linha.encode())
        return json.loads(linha)

    # ── Thread de leitura ───────────────────────────────────────

    def _loop_leitura(self):
        """Única thread que lê do socket. Classifica e despacha mensagens."""
        while self._ativa:
            try:
                msg = self._recv_linha()
            except Exception as e:
                if self._ativa:
                    log.warning(f"Conexão encerrada: {e}")
                    self.fila_eventos.put({"tipo": "_desconectado"})
                break

            tipo = msg.get("tipo")

            # Mensagens assíncronas (push do servidor)
            if tipo in ("alerta", "status"):
                self.fila_eventos.put(msg)

            # Tudo mais é resposta a um pedido da GUI
            else:
                self.fila_respostas.put(msg)

    def get_resposta(self, timeout=5):
        """GUI chama isto após send para pegar o ACK. Lança queue.Empty se timeout."""
        return self.fila_respostas.get(timeout=timeout)

    # ── API pública ─────────────────────────────────────────────

    def conectar(self, sensor_id, lab, tipo_sensor):
        self._buf  = ""
        self.sock  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(10)
        self.sock.connect((HOST, PORT_TCP))

        # handshake de registro (ainda síncrono, antes da thread)
        self._send_raw({
            "tipo": "registro",
            "sensor_id": sensor_id,
            "lab": lab,
            "tipo_sensor": tipo_sensor,
        })
        # lê ACK diretamente (thread ainda não subiu)
        resp = self._recv_linha()
        if resp.get("status") != "ok":
            self.sock.close()
            return False

        self.sock.settimeout(None)
        self._ativa = True
        self._thread = Thread(target=self._loop_leitura, daemon=True)
        self._thread.start()
        log.info(f"Conectado como {sensor_id} no lab {lab}")
        return True

    def desconectar(self):
        self._ativa = False
        try:
            self._send_raw({"tipo": "quit"})
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass

    def enviar_leitura(self, valor, tipo_sensor):
        unidade = UNIDADES.get(tipo_sensor, "")
        t_envio = time.time()
        self._send_raw({
            "tipo":    "leitura",
            "valor":   valor,
            "unidade": unidade,
            "t_envio": t_envio,
        })
        resp   = self.get_resposta(timeout=5)
        rtt_ms = resp.get("rtt_ms", 0)
        self.rtts.append(rtt_ms)
        self.total_leituras += 1
        return rtt_ms, resp.get("alerta")

    def ping(self):
        t = time.time()
        self._send_raw({"tipo": "ping", "t_envio": t})
        resp = self.get_resposta(timeout=5)
        return resp.get("rtt_ms", 0)

    def metricas_resumo(self):
        if self.rtts:
            return {
                "min": round(min(self.rtts), 2),
                "med": round(sum(self.rtts) / len(self.rtts), 2),
                "max": round(max(self.rtts), 2),
            }
        return None


# ════════════════════════════════════════════════════════════════
#  HELPERS DE AUTENTICAÇÃO (conexões temporárias independentes)
# ════════════════════════════════════════════════════════════════

def _req_temp(payload, timeout=5):
    """Abre socket, envia payload, lê resposta e fecha."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((HOST, PORT_TCP))
        s.sendall((json.dumps(payload) + "\n").encode())
        data = b""
        while b"\n" not in data:
            data += s.recv(1024)
        return json.loads(data.split(b"\n")[0])
    finally:
        s.close()


def cadastrar_usuario_tcp(nome, email, senha):
    return _req_temp({"tipo": "cadastro", "nome": nome,
                      "email": email, "senha": senha})


def login_usuario_tcp(email, senha):
    return _req_temp({"tipo": "login", "email": email, "senha": senha})


# ════════════════════════════════════════════════════════════════
#  JANELAS GUI
# ════════════════════════════════════════════════════════════════

def janela_login():
    layout = [
        [sg.Text("Monitoramento de Laboratório - UnB", font=FONT_TITULO,
                 text_color="#4fc3f7", justification="center", expand_x=True)],
        [sg.HSeparator()],
        [sg.Text("E-mail:", size=8, font=FONT_NORMAL),
         sg.Input(key="-EMAIL-", size=30, font=FONT_NORMAL)],
        [sg.Text("Senha:", size=8, font=FONT_NORMAL),
         sg.Input(key="-SENHA-", size=30, password_char="*", font=FONT_NORMAL)],
        [sg.HSeparator()],
        [sg.Button("Entrar", size=10, button_color=("#fff", "#0077b6")),
         sg.Button("Cadastrar", size=10),
         sg.Button("Sair", size=10)],
        [sg.Text("", key="-MSG-", text_color="#e94560", font=FONT_NORMAL)],
    ]
    return sg.Window("Login - Monitoramento Lab", layout, finalize=True,
                     element_justification="center")


def janela_cadastro():
    layout = [
        [sg.Text("Novo Usuário", font=FONT_TITULO, justification="center",
                 expand_x=True)],
        [sg.Text("Nome:", size=8),  sg.Input(key="-NOME-", size=30)],
        [sg.Text("E-mail:", size=8), sg.Input(key="-EMAIL-", size=30)],
        [sg.Text("Senha:", size=8),  sg.Input(key="-SENHA-", size=30,
                                               password_char="*")],
        [sg.Button("Cadastrar"), sg.Button("Cancelar")],
        [sg.Text("", key="-MSG-", text_color="#e94560")],
    ]
    return sg.Window("Cadastro", layout, finalize=True,
                     element_justification="center")


def janela_inicial(usuario):
    sensores = ["temperatura", "umidade", "co2", "cpu"]
    layout = [
        [sg.Text(f"Bem-vindo(a), {usuario}!", font=FONT_TITULO,
                 text_color="#4fc3f7")],
        [sg.HSeparator()],
        [sg.Text("Laboratório:", font=FONT_NORMAL),
         sg.Input("lab-1", key="-LAB-", size=15, font=FONT_NORMAL)],
        [sg.Text("ID do Sensor:", font=FONT_NORMAL),
         sg.Input("sensor-01", key="-SENSOR_ID-", size=15, font=FONT_NORMAL)],
        [sg.Text("Tipo de Sensor:", font=FONT_NORMAL)],
        [sg.Listbox(values=sensores, size=(22, 5), key="-TIPO-",
                    enable_events=True, font=FONT_NORMAL,
                    select_mode=sg.LISTBOX_SELECT_MODE_SINGLE)],
        [sg.Button("Conectar e Monitorar", size=20,
                   button_color=("#fff", "#0077b6")),
         sg.Button("Sair")],
        [sg.Text("", key="-MSG-", text_color="#e94560")],
    ]
    return sg.Window("Selecionar Sensor", layout, finalize=True)


def janela_monitoramento(tipo_sensor, sensor_id, lab):
    unidade = UNIDADES.get(tipo_sensor, "")
    vmin, vmax = LIMITES.get(tipo_sensor, (0, 100))
    titulos = {
        "temperatura": "Temperatura",
        "umidade":     "Umidade",
        "co2":         "Ventilação",
        "cpu":         "CPU das Máquinas",
    }
    titulo = titulos.get(tipo_sensor, tipo_sensor.capitalize())

    layout = [
        [sg.Text(titulo, font=FONT_TITULO, text_color="#4fc3f7"),
         sg.Push(),
         sg.Text(f"Sensor: {sensor_id} | Lab: {lab}", font=FONT_NORMAL,
                 text_color="#aaa")],
        [sg.HSeparator()],

        [sg.Frame("Enviar Leitura Manual", [
            [sg.Text(f"Valor ({unidade}):", font=FONT_NORMAL),
             sg.Input("", key="-VALOR-", size=10, font=FONT_NORMAL),
             sg.Button("Enviar", key="-ENVIAR-",
                       button_color=("#fff", "#0077b6"))],
        ], font=FONT_NORMAL)],

        [sg.Frame("Envio Automático (simulação)", [
            [sg.Text("Intervalo (s):", font=FONT_NORMAL),
             sg.Spin([i for i in range(1, 61)], initial_value=5,
                     key="-INTERVALO-", size=5, font=FONT_NORMAL),
             sg.Button("Iniciar Auto", key="-AUTO-",
                       button_color=("#fff", "#2e7d32")),
             sg.Button("Parar Auto", key="-PARAR-",
                       button_color=("#fff", "#c62828"))],
            [sg.Text(f"Faixa: {vmin}–{vmax} {unidade} (±20% para gerar alertas)",
                     font=("Helvetica", 9), text_color="#aaa")],
        ], font=FONT_NORMAL)],

        [sg.Frame("Status Atual", [
            [sg.Text("Valor:", font=FONT_NORMAL),
             sg.Text("-", key="-STATUS_VALOR-",
                     font=("Helvetica", 16, "bold"),
                     text_color="#4fc3f7", size=12)],
            [sg.Text("Status:", font=FONT_NORMAL),
             sg.Text("—", key="-STATUS_OK-", font=FONT_NORMAL, size=14)],
            [sg.Text("RTT:", font=FONT_NORMAL),
             sg.Text("-", key="-RTT-", font=FONT_NORMAL, size=12)],
        ], font=FONT_NORMAL)],

        [sg.Frame("Métricas de Rede", [
            [sg.Text("Leituras enviadas:", font=FONT_NORMAL),
             sg.Text("0", key="-M_LEITURAS-", font=FONT_MONO, size=8)],
            [sg.Text("RTT mín/méd/máx (ms):", font=FONT_NORMAL),
             sg.Text("-", key="-M_RTT-", font=FONT_MONO, size=20)],
            [sg.Text("Bytes TX / RX:", font=FONT_NORMAL),
             sg.Text("0 / 0", key="-M_TXRX-", font=FONT_MONO, size=16)],
            [sg.Button("Medir RTT (PING)", key="-PING-")],
        ], font=FONT_NORMAL)],

        [sg.Frame("Log de Eventos", [
            [sg.Multiline("", key="-LOG-", size=(70, 8), autoscroll=True,
                          disabled=True, font=FONT_MONO,
                          background_color="#0d1117",
                          text_color="#c9d1d9")],
        ], font=FONT_NORMAL)],

        [sg.Button("Desconectar", button_color=("#fff", "#c62828")),
         sg.Push(),
         sg.Text("http://127.0.0.1:8080/painel",
                 font=("Helvetica", 9), text_color="#aaa", key="-LINK-", tooltip="clique para abrir no navegador")],
    ]
    return sg.Window(f"Monitorando - {titulo}", layout,
                     finalize=True, size=(680, 660))


# ════════════════════════════════════════════════════════════════
#  PAINEL DE MONITORAMENTO
# ════════════════════════════════════════════════════════════════

def _log(window, msg):
    ts = datetime.now().strftime("%H:%M:%S")
    window["-LOG-"].print(f"[{ts}] {msg}")


def _atualizar_metricas(window, cx):
    window["-M_LEITURAS-"].update(str(cx.total_leituras))
    m = cx.metricas_resumo()
    if m:
        window["-M_RTT-"].update(f"{m['min']} / {m['med']} / {m['max']}")
    window["-M_TXRX-"].update(f"{cx.bytes_enviados} B / {cx.bytes_recebidos} B")


def _processar_leitura(window, cx, valor, tipo_sensor, prefixo=""):
    """Envia leitura e atualiza a GUI. Retorna False em erro de rede."""
    vmin, vmax = LIMITES.get(tipo_sensor, (0, 100))
    unidade    = UNIDADES.get(tipo_sensor, "")
    try:
        rtt, alerta = cx.enviar_leitura(valor, tipo_sensor)
    except queue.Empty:
        _log(window, "Timeout aguardando resposta do servidor.")
        return False
    except Exception as e:
        _log(window, f"Erro de rede: {e}")
        return False

    status = "OK" if vmin <= valor <= vmax else "FORA DA FAIXA"
    window["-STATUS_VALOR-"].update(f"{valor} {unidade}")
    window["-STATUS_OK-"].update(
        status,
        text_color="#4caf50" if "OK" in status else "#e94560"
    )
    window["-RTT-"].update(f"{rtt} ms")
    _log(window, f"{prefixo}{valor}{unidade} | RTT={rtt}ms | {status}")
    if alerta:
        _log(window, f"ALERTA do servidor: {alerta}")
    _atualizar_metricas(window, cx)
    return True


def executar_painel(tipo_sensor, sensor_id, lab, cx: ConexaoSensor):
    window     = janela_monitoramento(tipo_sensor, sensor_id, lab)
    vmin, vmax = LIMITES.get(tipo_sensor, (0, 100))
    auto_stop  = threading.Event()
    auto_ativo = False

    def _auto_loop(intervalo, stop_ev):
        while not stop_ev.is_set():
            if random.random() < 0.2:
                valor = round(random.uniform(vmax * 1.05, vmax * 1.3), 2)
            else:
                valor = round(random.uniform(max(0, vmin * 0.9) + 0.1,
                                             vmax * 0.95), 2)
            window.write_event_value("-VALOR_AUTO-", valor)
            stop_ev.wait(intervalo)

    def _poll_eventos():
        """Verifica fila_eventos a cada ciclo do timeout e injeta na janela."""
        try:
            while True:
                msg = cx.fila_eventos.get_nowait()
                if msg.get("tipo") == "alerta":
                    window.write_event_value("-ALERTA_RECV-", msg)
                elif msg.get("tipo") == "status":
                    window.write_event_value("-STATUS_RECV-", msg)
                elif msg.get("tipo") == "_desconectado":
                    window.write_event_value("-NET_CAIU-", {})
        except queue.Empty:
            pass

    while True:
        event, values = window.read(timeout=150)
        _poll_eventos()   # verifica alertas assíncronos

        if event in (sg.WIN_CLOSED, "Desconectar"):
            break

        # ── Envio manual ──
        elif event == "-ENVIAR-":
            raw = values["-VALOR-"].strip()
            if not raw:
                _log(window, "Informe um valor antes de enviar.")
                continue
            try:
                valor = float(raw)
            except ValueError:
                _log(window, "Valor inválido — use número.")
                continue
            _processar_leitura(window, cx, valor, tipo_sensor)

        # ── Valor automático ──
        elif event == "-VALOR_AUTO-":
            ok = _processar_leitura(window, cx, values[event],
                                    tipo_sensor, prefixo="[AUTO] ")
            if not ok:
                auto_stop.set()
                auto_ativo = False

        # ── Controle do automático ──
        elif event == "-AUTO-":
            if not auto_ativo:
                intervalo = int(values["-INTERVALO-"])
                auto_stop.clear()
                Thread(target=_auto_loop, args=(intervalo, auto_stop),
                       daemon=True).start()
                auto_ativo = True
                _log(window, f"Automático iniciado (intervalo={intervalo}s)")

        elif event == "-PARAR-":
            if auto_ativo:
                auto_stop.set()
                auto_ativo = False
                _log(window, "Automático parado")

        # ── PING ──
        elif event == "-PING-":
            try:
                rtt = cx.ping()
                window["-RTT-"].update(f"{rtt} ms (PING)")
                _log(window, f"PING → PONG | RTT={rtt}ms")
                _atualizar_metricas(window, cx)
            except queue.Empty:
                _log(window, "Timeout no PING.")
            except Exception as e:
                _log(window, f"Erro no PING: {e}")

        # ── Alertas e status recebidos assincronamente ──
        elif event == "-ALERTA_RECV-":
            msg = values[event]
            _log(window,
                 f"ALERTA de {msg.get('sensor_id')}: {msg.get('mensagem')}")

        elif event == "-STATUS_RECV-":
            msg = values[event]
            _log(window,
                 f"Sensor {msg.get('sensor_id')} {msg.get('evento','atualizou')}")

        elif event == "-NET_CAIU-":
            _log(window, "Conexão com o servidor perdida.")
            break

        # ── Acessar o painel web ──
        elif event == "-LINK-":
            webbrowser.open("http://127.0.0.1:8080/painel")
    auto_stop.set()
    window.close()


# ════════════════════════════════════════════════════════════════
#  FLUXO PRINCIPAL
# ════════════════════════════════════════════════════════════════

def main():
    usuario_logado = None

    # ── Login ──
    while True:
        win = janela_login()
        while True:
            event, values = win.read()

            if event in (sg.WIN_CLOSED, "Sair"):
                win.close()
                return

            elif event == "Cadastrar":
                win.close()
                wc = janela_cadastro()
                while True:
                    ev2, val2 = wc.read()
                    if ev2 in (sg.WIN_CLOSED, "Cancelar"):
                        break
                    elif ev2 == "Cadastrar":
                        nome  = val2["-NOME-"].strip()
                        email = val2["-EMAIL-"].strip()
                        senha = val2["-SENHA-"].strip()
                        if not (nome and email and senha):
                            wc["-MSG-"].update("Preencha todos os campos.")
                            continue
                        try:
                            resp = cadastrar_usuario_tcp(nome, email, senha)
                            if resp.get("status") == "ok":
                                sg.popup("Cadastro realizado!", title="OK")
                                break
                            else:
                                wc["-MSG-"].update(
                                    resp.get("msg", "Erro desconhecido"))
                        except Exception as e:
                            wc["-MSG-"].update(f"Erro de conexão: {e}")
                wc.close()
                win = janela_login()
                continue

            elif event == "Entrar":
                email = values["-EMAIL-"].strip()
                senha = values["-SENHA-"].strip()
                if not (email and senha):
                    win["-MSG-"].update("Preencha e-mail e senha.")
                    continue
                try:
                    resp = login_usuario_tcp(email, senha)
                    if resp.get("status") == "ok":
                        usuario_logado = resp.get("nome", email)
                        break
                    else:
                        win["-MSG-"].update("Credenciais inválidas.")
                except Exception as e:
                    win["-MSG-"].update(f"Servidor indisponível: {e}")

        win.close()
        if not usuario_logado:
            continue

        # ── Seleção de sensor ──
        while True:
            wi = janela_inicial(usuario_logado)
            sair_app = False
            while True:
                ev, val = wi.read()
                if ev in (sg.WIN_CLOSED, "Sair"):
                    sair_app = True
                    break

                elif ev == "Conectar e Monitorar":
                    tipo_sel = val.get("-TIPO-")
                    if not tipo_sel:
                        wi["-MSG-"].update("Selecione um tipo de sensor.")
                        continue
                    tipo_sensor = tipo_sel[0]
                    lab         = val["-LAB-"].strip() or "lab-1"
                    sensor_id   = val["-SENSOR_ID-"].strip() or "sensor-01"

                    wi["-MSG-"].update("Conectando...")
                    wi.refresh()

                    cx = ConexaoSensor()
                    try:
                        ok = cx.conectar(sensor_id, lab, tipo_sensor)
                    except Exception as e:
                        wi["-MSG-"].update(f"Erro: {e}")
                        continue

                    if ok:
                        wi.close()
                        executar_painel(tipo_sensor, sensor_id, lab, cx)
                        cx.desconectar()
                        break
                    else:
                        wi["-MSG-"].update("Falha no registro. Tente novamente.")

            if sair_app:
                wi.close()
                return


if __name__ == "__main__":
    main()
