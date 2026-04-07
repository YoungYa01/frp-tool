import copy
import io
import os
import sys
from typing import Any, Dict, List, Optional

try:
    import tomllib as tomli
except ImportError:  # pragma: no cover
    import tomli  # type: ignore

import tomli_w

try:
    import winreg  # type: ignore
except ImportError:  # pragma: no cover
    winreg = None

from PyQt5.QtCore import QObject, QProcess, Qt, pyqtSignal
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QAbstractItemView,
)


ROOT_KNOWN_KEYS = {
    "auth",
    "user",
    "serverAddr",
    "serverPort",
    "natHoleStunServer",
    "dnsServer",
    "loginFailExit",
    "start",
    "log",
    "webServer",
    "transport",
    "virtualNet",
    "featureGates",
    "udpPacketSize",
    "metadatas",
    "includes",
    "store",
    "proxies",
    "visitors",
}

PROXY_KNOWN_KEYS = {
    "name",
    "type",
    "enabled",
    "annotations",
    "transport",
    "metadatas",
    "loadBalancer",
    "healthCheck",
    "localIP",
    "localPort",
    "plugin",
    "remotePort",
    "customDomains",
    "subdomain",
    "locations",
    "httpUser",
    "httpPassword",
    "hostHeaderRewrite",
    "requestHeaders",
    "responseHeaders",
    "routeByHTTPUser",
    "multiplexer",
    "secretKey",
    "allowUsers",
}

VISITOR_KNOWN_KEYS = {
    "name",
    "type",
    "enabled",
    "transport",
    "secretKey",
    "serverUser",
    "serverName",
    "bindAddr",
    "bindPort",
    "plugin",
    "protocol",
    "keepTunnelOpen",
    "maxRetriesAnHour",
    "minRetryInterval",
    "fallbackTo",
    "fallbackTimeoutMs",
    "natTraversal",
}




class FrpProcessManager(QObject):
    log_signal = pyqtSignal(str)
    status_signal = pyqtSignal(bool)

    def __init__(self, exe_path: Optional[str] = None, config_path: Optional[str] = None):
        super().__init__()
        self.exe_path = exe_path or self.find_frpc_exe()
        self.config_path = config_path or os.path.join(app_dir(), "frpc.toml")
        self.process = QProcess(self)
        self.is_running = False
        self._helper_processes: List[QProcess] = []

        self.process.readyReadStandardOutput.connect(self._on_stdout)
        self.process.readyReadStandardError.connect(self._on_stderr)
        self.process.started.connect(self._on_started)
        self.process.finished.connect(self._on_finished)
        self.process.errorOccurred.connect(self._on_error)
        self.process.setWorkingDirectory(app_dir())

    @staticmethod
    def find_frpc_exe() -> str:
        candidates = [
            os.path.join(app_dir(), "frpc.exe"),
            os.path.join(os.getcwd(), "frpc.exe"),
            "frpc.exe",
        ]
        for candidate in candidates:
            if os.path.isabs(candidate) and os.path.exists(candidate):
                return candidate
        return candidates[0]

    def set_paths(self, exe_path: Optional[str] = None, config_path: Optional[str] = None) -> None:
        if exe_path:
            self.exe_path = exe_path
        if config_path:
            self.config_path = config_path
        self.process.setWorkingDirectory(app_dir())

    def start(self) -> None:
        if self.is_running:
            self.log_signal.emit("[INFO] frpc 已在运行，无需重复启动")
            return
        if not os.path.exists(self.exe_path):
            self.log_signal.emit(f"[ERROR] 找不到 frpc 可执行文件: {self.exe_path}")
            return
        if not os.path.exists(self.config_path):
            self.log_signal.emit(f"[ERROR] 找不到配置文件: {self.config_path}")
            return
        self.process.setWorkingDirectory(os.path.dirname(self.exe_path) or app_dir())
        self.process.start(self.exe_path, ["-c", self.config_path])

    def stop(self) -> None:
        if not self.is_running:
            self.log_signal.emit("[INFO] frpc 未运行")
            return
        self.log_signal.emit("[INFO] 正在停止 frpc...")
        self.process.terminate()
        if not self.process.waitForFinished(3000):
            self.log_signal.emit("[WARN] terminate 超时，改为 kill")
            self.process.kill()
            self.process.waitForFinished(3000)

    def reload_config(self) -> None:
        self._run_helper(["reload", "-c", self.config_path], "reload")

    def show_status(self) -> None:
        self._run_helper(["status", "-c", self.config_path], "status")

    def _run_helper(self, args: List[str], title: str) -> None:
        if not os.path.exists(self.exe_path):
            self.log_signal.emit(f"[ERROR] 找不到 frpc 可执行文件: {self.exe_path}")
            return
        helper = QProcess(self)
        helper.setWorkingDirectory(os.path.dirname(self.exe_path) or app_dir())
        helper.readyReadStandardOutput.connect(
            lambda h=helper, t=title: self._read_helper_output(h, t, stderr=False)
        )
        helper.readyReadStandardError.connect(
            lambda h=helper, t=title: self._read_helper_output(h, t, stderr=True)
        )
        helper.errorOccurred.connect(lambda err, t=title: self.log_signal.emit(f"[ERROR] {t} 失败: {err}"))
        helper.finished.connect(lambda code, status, h=helper, t=title: self._finish_helper(h, t, code, status))
        self._helper_processes.append(helper)
        self.log_signal.emit(f"[INFO] 执行 frpc {title} ...")
        helper.start(self.exe_path, args)

    def _read_helper_output(self, helper: QProcess, title: str, stderr: bool = False) -> None:
        if stderr:
            data = helper.readAllStandardError().data().decode("utf-8", errors="ignore")
            if data.strip():
                self.log_signal.emit(f"[{title}][stderr] {data.strip()}")
        else:
            data = helper.readAllStandardOutput().data().decode("utf-8", errors="ignore")
            if data.strip():
                self.log_signal.emit(f"[{title}] {data.strip()}")

    def _finish_helper(self, helper: QProcess, title: str, exit_code: int, exit_status: QProcess.ExitStatus) -> None:
        status_text = "NormalExit" if exit_status == QProcess.NormalExit else "CrashExit"
        self.log_signal.emit(f"[INFO] frpc {title} 结束，退出码={exit_code}，状态={status_text}")
        if helper in self._helper_processes:
            self._helper_processes.remove(helper)
        helper.deleteLater()

    def _on_started(self) -> None:
        self.is_running = True
        self.status_signal.emit(True)
        self.log_signal.emit(f"[INFO] frpc 已启动: {self.exe_path} -c {self.config_path}")

    def _on_finished(self, exit_code: int, exit_status: QProcess.ExitStatus) -> None:
        self.is_running = False
        self.status_signal.emit(False)
        status_text = "NormalExit" if exit_status == QProcess.NormalExit else "CrashExit"
        self.log_signal.emit(f"[INFO] frpc 已停止，退出码={exit_code}，状态={status_text}")

    def _on_stdout(self) -> None:
        data = self.process.readAllStandardOutput().data().decode("utf-8", errors="ignore")
        if data.strip():
            self.log_signal.emit(data.strip())

    def _on_stderr(self) -> None:
        data = self.process.readAllStandardError().data().decode("utf-8", errors="ignore")
        if data.strip():
            self.log_signal.emit(data.strip())

    def _on_error(self, error: QProcess.ProcessError) -> None:
        self.log_signal.emit(f"[ERROR] frpc 进程错误: {error}")


class FrpManagerWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Frp 客户端管理器（增强版）")
        self.resize(1280, 900)

        self.cfg_mgr = ConfigManager()
        self.proc_mgr = FrpProcessManager(config_path=self.cfg_mgr.config_path)
        self.current_config = self.cfg_mgr.load_config()

        self.proxies: List[Dict[str, Any]] = []
        self.visitors: List[Dict[str, Any]] = []
        self.current_proxy_index = -1
        self.current_visitor_index = -1
        self._loading_proxy_editor = False
        self._loading_visitor_editor = False
        self._syncing_current_item = False

        self._init_ui()
        self._connect_signals()
        self._load_config_to_ui(self.current_config)
        self._check_autostart()
        self._update_preview()

    # ---------- UI ----------
    def _init_ui(self) -> None:
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        root_layout = QVBoxLayout(central_widget)

        ctrl_bar = QHBoxLayout()
        self.btn_save_cfg = QPushButton("保存配置")
        self.btn_reload_file = QPushButton("从文件重载")
        self.btn_start = QPushButton("保存并启动")
        self.btn_stop = QPushButton("停止 frpc")
        self.btn_reload_runtime = QPushButton("热重载")
        self.btn_status = QPushButton("查看代理状态")
        self.btn_stop.setEnabled(False)

        self.lbl_status = QLabel("状态: 未运行")
        self.lbl_status.setStyleSheet("color: red; font-weight: bold;")
        self.lbl_paths = QLabel(f"frpc: {self.proc_mgr.exe_path} | 配置: {self.cfg_mgr.config_path}")
        self.lbl_paths.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.chk_autostart = QCheckBox("开机自启动")

        for widget in [
            self.btn_save_cfg,
            self.btn_reload_file,
            self.btn_start,
            self.btn_stop,
            self.btn_reload_runtime,
            self.btn_status,
            self.lbl_status,
        ]:
            ctrl_bar.addWidget(widget)
        ctrl_bar.addStretch()
        ctrl_bar.addWidget(self.chk_autostart)
        root_layout.addLayout(ctrl_bar)
        root_layout.addWidget(self.lbl_paths)

        self.tabs = QTabWidget()
        root_layout.addWidget(self.tabs)

        self.tabs.addTab(self._build_common_tab(), "通用配置")
        self.tabs.addTab(self._build_proxy_tab(), "代理 Proxies")
        self.tabs.addTab(self._build_visitor_tab(), "Visitor")
        self.tabs.addTab(self._build_preview_tab(), "配置预览")
        self.tabs.addTab(self._build_log_tab(), "运行日志")

    def _build_scroll_tab(self, content_widget: QWidget) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(content_widget)
        layout.addWidget(scroll)
        return tab

    def _build_common_tab(self) -> QWidget:
        content = QWidget()
        layout = QVBoxLayout(content)

        layout.addWidget(self._build_general_group())
        layout.addWidget(self._build_auth_group())
        layout.addWidget(self._build_transport_group())
        layout.addWidget(self._build_webserver_group())
        layout.addWidget(self._build_log_group())
        layout.addWidget(self._build_advanced_group())
        layout.addStretch()
        return self._build_scroll_tab(content)

    def _build_general_group(self) -> QGroupBox:
        group = QGroupBox("客户端公共配置")
        form = QFormLayout(group)

        self.le_server_addr = QLineEdit()
        self.sb_server_port = self._port_spinbox(default=7000)
        self.le_user = QLineEdit()
        self.le_dns_server = QLineEdit()
        self.le_stun_server = QLineEdit()
        self.chk_login_fail_exit = QCheckBox("首登失败即退出")
        self.chk_login_fail_exit.setChecked(True)
        self.te_start = QPlainTextEdit()
        self.te_start.setPlaceholderText("按行或逗号填写要启用的代理名，例如\nweb\nssh")
        self.te_includes = QPlainTextEdit()
        self.te_includes.setPlaceholderText("附加配置目录/文件（只读取 proxy / visitor 配置）")
        self.sb_udp_packet_size = QSpinBox()
        self.sb_udp_packet_size.setRange(0, 65535)
        self.sb_udp_packet_size.setSpecialValueText("未设置")
        self.le_store_path = QLineEdit()
        self.le_virtual_net = QLineEdit()
        self.te_root_metadatas = QPlainTextEdit()
        self.te_root_feature_gates = QPlainTextEdit()
        self.te_root_extras = QPlainTextEdit()
        self.te_root_extras.setPlaceholderText("根级额外字段 JSON 对象，用于保留/补充 GUI 未覆盖的参数")

        form.addRow("serverAddr", self.le_server_addr)
        form.addRow("serverPort", self.sb_server_port)
        form.addRow("user", self.le_user)
        form.addRow("dnsServer", self.le_dns_server)
        form.addRow("natHoleStunServer", self.le_stun_server)
        form.addRow("loginFailExit", self.chk_login_fail_exit)
        form.addRow("start", self.te_start)
        form.addRow("includes", self.te_includes)
        form.addRow("udpPacketSize", self.sb_udp_packet_size)
        form.addRow("store.path", self.le_store_path)
        form.addRow("virtualNet.address", self.le_virtual_net)
        form.addRow("metadatas (key=value)", self.te_root_metadatas)
        form.addRow("featureGates (key=true/false)", self.te_root_feature_gates)
        form.addRow("extra JSON", self.te_root_extras)
        return group

    def _build_auth_group(self) -> QGroupBox:
        group = QGroupBox("认证配置 auth")
        grid = QGridLayout(group)

        self.cb_auth_method = QComboBox()
        self.cb_auth_method.addItems(["token", "oidc"])
        self.le_token = QLineEdit()
        self.le_token.setEchoMode(QLineEdit.PasswordEchoOnEdit)
        self.te_auth_token_source = QPlainTextEdit()
        self.te_auth_token_source.setPlaceholderText(
            '{"type":"file","file":{"path":"C:/secret/client_token.txt"}}\n或 exec 形式 ValueSource'
        )
        self.chk_scope_heartbeats = QCheckBox("HeartBeats")
        self.chk_scope_new_work_conns = QCheckBox("NewWorkConns")

        self.le_oidc_client_id = QLineEdit()
        self.le_oidc_client_secret = QLineEdit()
        self.le_oidc_client_secret.setEchoMode(QLineEdit.PasswordEchoOnEdit)
        self.le_oidc_audience = QLineEdit()
        self.le_oidc_scope = QLineEdit()
        self.le_oidc_token_endpoint = QLineEdit()
        self.te_oidc_additional_params = QPlainTextEdit()
        self.le_oidc_trusted_ca = QLineEdit()
        self.chk_oidc_skip_verify = QCheckBox("insecureSkipVerify")
        self.le_oidc_proxy_url = QLineEdit()
        self.te_oidc_token_source = QPlainTextEdit()
        self.te_oidc_token_source.setPlaceholderText("OIDC 动态 tokenSource JSON（配置后将覆盖其他 OIDC 字段）")

        row = 0
        grid.addWidget(QLabel("auth.method"), row, 0)
        grid.addWidget(self.cb_auth_method, row, 1)
        row += 1
        grid.addWidget(QLabel("auth.token"), row, 0)
        grid.addWidget(self.le_token, row, 1)
        row += 1
        grid.addWidget(QLabel("auth.tokenSource JSON"), row, 0)
        grid.addWidget(self.te_auth_token_source, row, 1)
        row += 1
        scope_row = QHBoxLayout()
        scope_row.addWidget(self.chk_scope_heartbeats)
        scope_row.addWidget(self.chk_scope_new_work_conns)
        scope_row.addStretch()
        scope_widget = QWidget()
        scope_widget.setLayout(scope_row)
        grid.addWidget(QLabel("auth.additionalScopes"), row, 0)
        grid.addWidget(scope_widget, row, 1)
        row += 1
        grid.addWidget(QLabel("oidc.clientID"), row, 0)
        grid.addWidget(self.le_oidc_client_id, row, 1)
        row += 1
        grid.addWidget(QLabel("oidc.clientSecret"), row, 0)
        grid.addWidget(self.le_oidc_client_secret, row, 1)
        row += 1
        grid.addWidget(QLabel("oidc.audience"), row, 0)
        grid.addWidget(self.le_oidc_audience, row, 1)
        row += 1
        grid.addWidget(QLabel("oidc.scope"), row, 0)
        grid.addWidget(self.le_oidc_scope, row, 1)
        row += 1
        grid.addWidget(QLabel("oidc.tokenEndpointURL"), row, 0)
        grid.addWidget(self.le_oidc_token_endpoint, row, 1)
        row += 1
        grid.addWidget(QLabel("oidc.additionalEndpointParams\n(key=value)"), row, 0)
        grid.addWidget(self.te_oidc_additional_params, row, 1)
        row += 1
        grid.addWidget(QLabel("oidc.trustedCaFile"), row, 0)
        grid.addWidget(self.le_oidc_trusted_ca, row, 1)
        row += 1
        grid.addWidget(QLabel("oidc.insecureSkipVerify"), row, 0)
        grid.addWidget(self.chk_oidc_skip_verify, row, 1)
        row += 1
        grid.addWidget(QLabel("oidc.proxyURL"), row, 0)
        grid.addWidget(self.le_oidc_proxy_url, row, 1)
        row += 1
        grid.addWidget(QLabel("oidc.tokenSource JSON"), row, 0)
        grid.addWidget(self.te_oidc_token_source, row, 1)
        return group

    def _build_transport_group(self) -> QGroupBox:
        group = QGroupBox("传输层 transport / TLS")
        form = QFormLayout(group)

        self.cb_transport_protocol = QComboBox()
        self.cb_transport_protocol.addItems(["tcp", "kcp", "quic", "websocket", "wss"])
        self.sb_dial_server_timeout = self._spinbox_with_unset(0, 3600)
        self.sb_dial_server_keepalive = self._spinbox_with_unset(0, 3600)
        self.le_connect_server_local_ip = QLineEdit()
        self.le_transport_proxy_url = QLineEdit()
        self.sb_pool_count = self._spinbox_with_unset(0, 1000)
        self.chk_tcp_mux = QCheckBox("tcpMux")
        self.chk_tcp_mux.setChecked(True)
        self.sb_tcp_mux_keepalive = self._spinbox_with_unset(0, 3600)
        self.sb_heartbeat_interval = self._spinbox_with_unset(-1, 3600, default=-1, special_text="未设置/-1")
        self.sb_heartbeat_timeout = self._spinbox_with_unset(0, 3600)

        self.chk_tls_enable = QCheckBox("TLS enable")
        self.chk_tls_enable.setChecked(True)
        self.chk_tls_disable_custom_byte = QCheckBox("disableCustomTLSFirstByte")
        self.chk_tls_disable_custom_byte.setChecked(True)
        self.le_tls_cert_file = QLineEdit()
        self.le_tls_key_file = QLineEdit()
        self.le_tls_trusted_ca = QLineEdit()
        self.le_tls_server_name = QLineEdit()

        self.sb_quic_keepalive = self._spinbox_with_unset(0, 3600)
        self.sb_quic_idle_timeout = self._spinbox_with_unset(0, 3600)
        self.sb_quic_max_streams = self._spinbox_with_unset(0, 1000000)

        form.addRow("transport.protocol", self.cb_transport_protocol)
        form.addRow("dialServerTimeout", self.sb_dial_server_timeout)
        form.addRow("dialServerKeepalive", self.sb_dial_server_keepalive)
        form.addRow("connectServerLocalIP", self.le_connect_server_local_ip)
        form.addRow("proxyURL", self.le_transport_proxy_url)
        form.addRow("poolCount", self.sb_pool_count)
        form.addRow("tcpMux", self.chk_tcp_mux)
        form.addRow("tcpMuxKeepaliveInterval", self.sb_tcp_mux_keepalive)
        form.addRow("heartbeatInterval", self.sb_heartbeat_interval)
        form.addRow("heartbeatTimeout", self.sb_heartbeat_timeout)
        form.addRow("tls.enable", self.chk_tls_enable)
        form.addRow("tls.disableCustomTLSFirstByte", self.chk_tls_disable_custom_byte)
        form.addRow("tls.certFile", self.le_tls_cert_file)
        form.addRow("tls.keyFile", self.le_tls_key_file)
        form.addRow("tls.trustedCaFile", self.le_tls_trusted_ca)
        form.addRow("tls.serverName", self.le_tls_server_name)
        form.addRow("quic.keepalivePeriod", self.sb_quic_keepalive)
        form.addRow("quic.maxIdleTimeout", self.sb_quic_idle_timeout)
        form.addRow("quic.maxIncomingStreams", self.sb_quic_max_streams)
        return group

    def _build_webserver_group(self) -> QGroupBox:
        group = QGroupBox("webServer（用于 frpc reload / status / 管理页面）")
        form = QFormLayout(group)

        self.le_web_addr = QLineEdit()
        self.sb_web_port = self._spinbox_with_unset(0, 65535)
        self.le_web_user = QLineEdit()
        self.le_web_password = QLineEdit()
        self.le_web_password.setEchoMode(QLineEdit.PasswordEchoOnEdit)
        self.le_web_assets_dir = QLineEdit()
        self.chk_web_pprof = QCheckBox("pprofEnable")
        self.le_web_tls_cert = QLineEdit()
        self.le_web_tls_key = QLineEdit()
        self.le_web_tls_ca = QLineEdit()
        self.le_web_tls_server_name = QLineEdit()

        form.addRow("webServer.addr", self.le_web_addr)
        form.addRow("webServer.port", self.sb_web_port)
        form.addRow("webServer.user", self.le_web_user)
        form.addRow("webServer.password", self.le_web_password)
        form.addRow("webServer.assetsDir", self.le_web_assets_dir)
        form.addRow("webServer.pprofEnable", self.chk_web_pprof)
        form.addRow("webServer.tls.certFile", self.le_web_tls_cert)
        form.addRow("webServer.tls.keyFile", self.le_web_tls_key)
        form.addRow("webServer.tls.trustedCaFile", self.le_web_tls_ca)
        form.addRow("webServer.tls.serverName", self.le_web_tls_server_name)
        return group

    def _build_log_group(self) -> QGroupBox:
        group = QGroupBox("日志 log")
        form = QFormLayout(group)

        self.le_log_to = QLineEdit()
        self.le_log_to.setPlaceholderText("console 或日志文件路径")
        self.cb_log_level = QComboBox()
        self.cb_log_level.addItems(["trace", "debug", "info", "warn", "error"])
        self.sb_log_max_days = self._spinbox_with_unset(0, 3650)
        self.chk_log_disable_color = QCheckBox("disablePrintColor")

        form.addRow("log.to", self.le_log_to)
        form.addRow("log.level", self.cb_log_level)
        form.addRow("log.maxDays", self.sb_log_max_days)
        form.addRow("log.disablePrintColor", self.chk_log_disable_color)
        return group

    def _build_advanced_group(self) -> QGroupBox:
        group = QGroupBox("说明")
        layout = QVBoxLayout(group)
        tip = QLabel(
            "1) frpc reload / status 依赖 webServer.port 已配置。\n"
            "2) token 与 tokenSource 互斥；OIDC tokenSource 配置后会覆盖其他 OIDC 字段。\n"
            "3) GUI 已覆盖 frp 常用项，遇到最新或特殊参数可填 extra JSON / plugin JSON。\n"
            "4) 默认会在当前脚本同级目录查找 frpc.exe 与 frpc.toml。"
        )
        tip.setWordWrap(True)
        layout.addWidget(tip)
        return group

    def _build_proxy_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        button_bar = QHBoxLayout()
        self.btn_add_proxy = QPushButton("新增代理")
        self.btn_copy_proxy = QPushButton("复制代理")
        self.btn_del_proxy = QPushButton("删除代理")
        self.btn_apply_proxy = QPushButton("应用当前代理修改")
        for widget in [self.btn_add_proxy, self.btn_copy_proxy, self.btn_del_proxy, self.btn_apply_proxy]:
            button_bar.addWidget(widget)
        button_bar.addStretch()
        layout.addLayout(button_bar)

        splitter = QSplitter(Qt.Horizontal)
        self.table_proxies = QTableWidget(0, 4)
        self.table_proxies.setHorizontalHeaderLabels(["名称", "类型", "启用", "摘要"])
        self.table_proxies.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_proxies.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_proxies.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table_proxies.setEditTriggers(QAbstractItemView.NoEditTriggers)
        splitter.addWidget(self.table_proxies)
        splitter.addWidget(self._build_proxy_editor())
        splitter.setSizes([420, 760])
        layout.addWidget(splitter)
        return tab

    def _build_proxy_editor(self) -> QWidget:
        content = QWidget()
        outer = QVBoxLayout(content)

        basic = QGroupBox("基础")
        form = QFormLayout(basic)
        self.le_proxy_name = QLineEdit()
        self.cb_proxy_type = QComboBox()
        self.cb_proxy_type.addItems(["tcp", "udp", "http", "https", "tcpmux", "stcp", "sudp", "xtcp"])
        self.chk_proxy_enabled = QCheckBox("enabled")
        self.chk_proxy_enabled.setChecked(True)
        self.le_proxy_local_ip = QLineEdit()
        self.sb_proxy_local_port = self._spinbox_with_unset(0, 65535)
        self.sb_proxy_remote_port = self._spinbox_with_unset(0, 65535)
        self.te_proxy_custom_domains = QPlainTextEdit()
        self.le_proxy_subdomain = QLineEdit()
        self.te_proxy_locations = QPlainTextEdit()
        self.le_proxy_http_user = QLineEdit()
        self.le_proxy_http_password = QLineEdit()
        self.le_proxy_http_password.setEchoMode(QLineEdit.PasswordEchoOnEdit)
        self.le_proxy_host_header_rewrite = QLineEdit()
        self.le_proxy_route_by_http_user = QLineEdit()
        self.cb_proxy_multiplexer = QComboBox()
        self.cb_proxy_multiplexer.setEditable(True)
        self.cb_proxy_multiplexer.addItems(["", "httpconnect"])
        self.le_proxy_secret_key = QLineEdit()
        self.le_proxy_secret_key.setEchoMode(QLineEdit.PasswordEchoOnEdit)
        self.te_proxy_allow_users = QPlainTextEdit()

        form.addRow("name", self.le_proxy_name)
        form.addRow("type", self.cb_proxy_type)
        form.addRow("enabled", self.chk_proxy_enabled)
        form.addRow("localIP", self.le_proxy_local_ip)
        form.addRow("localPort", self.sb_proxy_local_port)
        form.addRow("remotePort", self.sb_proxy_remote_port)
        form.addRow("customDomains", self.te_proxy_custom_domains)
        form.addRow("subdomain", self.le_proxy_subdomain)
        form.addRow("locations", self.te_proxy_locations)
        form.addRow("httpUser", self.le_proxy_http_user)
        form.addRow("httpPassword", self.le_proxy_http_password)
        form.addRow("hostHeaderRewrite", self.le_proxy_host_header_rewrite)
        form.addRow("routeByHTTPUser", self.le_proxy_route_by_http_user)
        form.addRow("multiplexer", self.cb_proxy_multiplexer)
        form.addRow("secretKey", self.le_proxy_secret_key)
        form.addRow("allowUsers", self.te_proxy_allow_users)
        outer.addWidget(basic)

        transport = QGroupBox("transport / backend / 插件")
        tform = QFormLayout(transport)
        self.chk_proxy_use_encryption = QCheckBox("useEncryption")
        self.chk_proxy_use_compression = QCheckBox("useCompression")
        self.le_proxy_bandwidth_limit = QLineEdit()
        self.cb_proxy_bandwidth_mode = QComboBox()
        self.cb_proxy_bandwidth_mode.setEditable(True)
        self.cb_proxy_bandwidth_mode.addItems(["", "client", "server"])
        self.cb_proxy_proxy_protocol = QComboBox()
        self.cb_proxy_proxy_protocol.setEditable(True)
        self.cb_proxy_proxy_protocol.addItems(["", "v1", "v2"])
        self.te_proxy_plugin_json = QPlainTextEdit()
        self.te_proxy_plugin_json.setPlaceholderText("Client plugin JSON，例如 static_file / http_proxy / https2http 等")
        tform.addRow("transport.useEncryption", self.chk_proxy_use_encryption)
        tform.addRow("transport.useCompression", self.chk_proxy_use_compression)
        tform.addRow("transport.bandwidthLimit", self.le_proxy_bandwidth_limit)
        tform.addRow("transport.bandwidthLimitMode", self.cb_proxy_bandwidth_mode)
        tform.addRow("transport.proxyProtocolVersion", self.cb_proxy_proxy_protocol)
        tform.addRow("plugin JSON", self.te_proxy_plugin_json)
        outer.addWidget(transport)

        lb_hc = QGroupBox("负载均衡 / 健康检查")
        lform = QFormLayout(lb_hc)
        self.le_proxy_lb_group = QLineEdit()
        self.le_proxy_lb_group_key = QLineEdit()
        self.cb_proxy_hc_type = QComboBox()
        self.cb_proxy_hc_type.setEditable(True)
        self.cb_proxy_hc_type.addItems(["", "tcp", "http"])
        self.sb_proxy_hc_timeout = self._spinbox_with_unset(0, 3600)
        self.sb_proxy_hc_max_failed = self._spinbox_with_unset(0, 1000)
        self.sb_proxy_hc_interval = self._spinbox_with_unset(0, 3600)
        self.le_proxy_hc_path = QLineEdit()
        self.te_proxy_hc_headers = QPlainTextEdit()
        lform.addRow("loadBalancer.group", self.le_proxy_lb_group)
        lform.addRow("loadBalancer.groupKey", self.le_proxy_lb_group_key)
        lform.addRow("healthCheck.type", self.cb_proxy_hc_type)
        lform.addRow("healthCheck.timeoutSeconds", self.sb_proxy_hc_timeout)
        lform.addRow("healthCheck.maxFailed", self.sb_proxy_hc_max_failed)
        lform.addRow("healthCheck.intervalSeconds", self.sb_proxy_hc_interval)
        lform.addRow("healthCheck.path", self.le_proxy_hc_path)
        lform.addRow("healthCheck.httpHeaders (key=value)", self.te_proxy_hc_headers)
        outer.addWidget(lb_hc)

        adv = QGroupBox("附加字段")
        aform = QFormLayout(adv)
        self.te_proxy_annotations = QPlainTextEdit()
        self.te_proxy_metadatas = QPlainTextEdit()
        self.te_proxy_request_headers = QPlainTextEdit()
        self.te_proxy_response_headers = QPlainTextEdit()
        self.te_proxy_extra_json = QPlainTextEdit()
        self.te_proxy_extra_json.setPlaceholderText("额外字段 JSON 对象，保留 GUI 未覆盖的 proxy 参数")
        aform.addRow("annotations (key=value)", self.te_proxy_annotations)
        aform.addRow("metadatas (key=value)", self.te_proxy_metadatas)
        aform.addRow("requestHeaders.set (key=value)", self.te_proxy_request_headers)
        aform.addRow("responseHeaders.set (key=value)", self.te_proxy_response_headers)
        aform.addRow("extra JSON", self.te_proxy_extra_json)
        outer.addWidget(adv)
        outer.addStretch()

        return self._build_scroll_tab(content)

    def _build_visitor_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        button_bar = QHBoxLayout()
        self.btn_add_visitor = QPushButton("新增 Visitor")
        self.btn_copy_visitor = QPushButton("复制 Visitor")
        self.btn_del_visitor = QPushButton("删除 Visitor")
        self.btn_apply_visitor = QPushButton("应用当前 Visitor 修改")
        for widget in [self.btn_add_visitor, self.btn_copy_visitor, self.btn_del_visitor, self.btn_apply_visitor]:
            button_bar.addWidget(widget)
        button_bar.addStretch()
        layout.addLayout(button_bar)

        splitter = QSplitter(Qt.Horizontal)
        self.table_visitors = QTableWidget(0, 4)
        self.table_visitors.setHorizontalHeaderLabels(["名称", "类型", "启用", "摘要"])
        self.table_visitors.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_visitors.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_visitors.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table_visitors.setEditTriggers(QAbstractItemView.NoEditTriggers)
        splitter.addWidget(self.table_visitors)
        splitter.addWidget(self._build_visitor_editor())
        splitter.setSizes([420, 760])
        layout.addWidget(splitter)
        return tab

    def _build_visitor_editor(self) -> QWidget:
        content = QWidget()
        outer = QVBoxLayout(content)

        basic = QGroupBox("基础")
        form = QFormLayout(basic)
        self.le_visitor_name = QLineEdit()
        self.cb_visitor_type = QComboBox()
        self.cb_visitor_type.addItems(["stcp", "sudp", "xtcp"])
        self.chk_visitor_enabled = QCheckBox("enabled")
        self.chk_visitor_enabled.setChecked(True)
        self.le_visitor_secret_key = QLineEdit()
        self.le_visitor_secret_key.setEchoMode(QLineEdit.PasswordEchoOnEdit)
        self.le_visitor_server_user = QLineEdit()
        self.le_visitor_server_name = QLineEdit()
        self.le_visitor_bind_addr = QLineEdit()
        self.sb_visitor_bind_port = self._spinbox_with_unset(-1, 65535, default=-1, special_text="-1（不监听物理端口）")
        form.addRow("name", self.le_visitor_name)
        form.addRow("type", self.cb_visitor_type)
        form.addRow("enabled", self.chk_visitor_enabled)
        form.addRow("secretKey", self.le_visitor_secret_key)
        form.addRow("serverUser", self.le_visitor_server_user)
        form.addRow("serverName", self.le_visitor_server_name)
        form.addRow("bindAddr", self.le_visitor_bind_addr)
        form.addRow("bindPort", self.sb_visitor_bind_port)
        outer.addWidget(basic)

        transport = QGroupBox("transport / xtcp")
        tform = QFormLayout(transport)
        self.chk_visitor_use_encryption = QCheckBox("useEncryption")
        self.chk_visitor_use_compression = QCheckBox("useCompression")
        self.cb_visitor_protocol = QComboBox()
        self.cb_visitor_protocol.setEditable(True)
        self.cb_visitor_protocol.addItems(["", "quic", "kcp"])
        self.chk_visitor_keep_tunnel_open = QCheckBox("keepTunnelOpen")
        self.sb_visitor_max_retries = self._spinbox_with_unset(0, 1000)
        self.sb_visitor_min_retry = self._spinbox_with_unset(0, 3600)
        self.le_visitor_fallback_to = QLineEdit()
        self.sb_visitor_fallback_timeout = self._spinbox_with_unset(0, 600000)
        self.chk_visitor_disable_assisted_addrs = QCheckBox("natTraversal.disableAssistedAddrs")
        self.te_visitor_plugin_json = QPlainTextEdit()
        self.te_visitor_plugin_json.setPlaceholderText("Visitor plugin JSON")
        tform.addRow("transport.useEncryption", self.chk_visitor_use_encryption)
        tform.addRow("transport.useCompression", self.chk_visitor_use_compression)
        tform.addRow("protocol", self.cb_visitor_protocol)
        tform.addRow("keepTunnelOpen", self.chk_visitor_keep_tunnel_open)
        tform.addRow("maxRetriesAnHour", self.sb_visitor_max_retries)
        tform.addRow("minRetryInterval", self.sb_visitor_min_retry)
        tform.addRow("fallbackTo", self.le_visitor_fallback_to)
        tform.addRow("fallbackTimeoutMs", self.sb_visitor_fallback_timeout)
        tform.addRow("natTraversal.disableAssistedAddrs", self.chk_visitor_disable_assisted_addrs)
        tform.addRow("plugin JSON", self.te_visitor_plugin_json)
        outer.addWidget(transport)

        adv = QGroupBox("附加字段")
        aform = QFormLayout(adv)
        self.te_visitor_extra_json = QPlainTextEdit()
        self.te_visitor_extra_json.setPlaceholderText("额外字段 JSON 对象，保留 GUI 未覆盖的 visitor 参数")
        aform.addRow("extra JSON", self.te_visitor_extra_json)
        outer.addWidget(adv)
        outer.addStretch()
        return self._build_scroll_tab(content)

    def _build_preview_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        button_bar = QHBoxLayout()
        self.btn_refresh_preview = QPushButton("刷新预览")
        button_bar.addWidget(self.btn_refresh_preview)
        button_bar.addStretch()
        self.txt_preview = QPlainTextEdit()
        self.txt_preview.setReadOnly(True)
        self.txt_preview.setFont(QFont("Consolas", 10))
        layout.addLayout(button_bar)
        layout.addWidget(self.txt_preview)
        return tab

    def _build_log_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        button_bar = QHBoxLayout()
        self.btn_clear_log = QPushButton("清空日志")
        button_bar.addWidget(self.btn_clear_log)
        button_bar.addStretch()
        self.txt_log = QTextEdit()
        self.txt_log.setReadOnly(True)
        self.txt_log.setFont(QFont("Consolas", 9))
        layout.addLayout(button_bar)
        layout.addWidget(self.txt_log)
        return tab

    def _connect_signals(self) -> None:
        self.btn_save_cfg.clicked.connect(self._save_config)
        self.btn_reload_file.clicked.connect(self._reload_from_file)
        self.btn_start.clicked.connect(self._save_and_start)
        self.btn_stop.clicked.connect(self.proc_mgr.stop)
        self.btn_reload_runtime.clicked.connect(self._reload_runtime)
        self.btn_status.clicked.connect(self._show_runtime_status)
        self.btn_refresh_preview.clicked.connect(self._update_preview)
        self.btn_clear_log.clicked.connect(self.txt_log.clear)

        self.btn_add_proxy.clicked.connect(self._add_proxy)
        self.btn_copy_proxy.clicked.connect(self._copy_proxy)
        self.btn_del_proxy.clicked.connect(self._delete_proxy)
        self.btn_apply_proxy.clicked.connect(self._apply_current_proxy)
        self.table_proxies.itemSelectionChanged.connect(self._on_proxy_selection_changed)

        self.btn_add_visitor.clicked.connect(self._add_visitor)
        self.btn_copy_visitor.clicked.connect(self._copy_visitor)
        self.btn_del_visitor.clicked.connect(self._delete_visitor)
        self.btn_apply_visitor.clicked.connect(self._apply_current_visitor)
        self.table_visitors.itemSelectionChanged.connect(self._on_visitor_selection_changed)

        self.proc_mgr.log_signal.connect(self._append_log)
        self.proc_mgr.status_signal.connect(self._update_status)

        self.chk_autostart.stateChanged.connect(self._toggle_autostart)

    # ---------- helpers ----------
    def _port_spinbox(self, default: int = 0) -> QSpinBox:
        widget = QSpinBox()
        widget.setRange(1, 65535)
        widget.setValue(default)
        return widget

    def _spinbox_with_unset(self, minimum: int, maximum: int, default: Optional[int] = None, special_text: str = "未设置") -> QSpinBox:
        widget = QSpinBox()
        widget.setRange(minimum, maximum)
        if minimum <= 0 <= maximum:
            widget.setSpecialValueText(special_text)
        widget.setValue(default if default is not None else minimum)
        return widget

    # ---------- load / save ----------
    def _load_config_to_ui(self, config: Dict[str, Any]) -> None:
        cfg = self.cfg_mgr.normalize_config(config)
        self.current_config = deep_copy(cfg)

        self.le_server_addr.setText(cfg.get("serverAddr", ""))
        self.sb_server_port.setValue(cfg.get("serverPort", 7000))
        self.le_user.setText(cfg.get("user", ""))
        self.le_dns_server.setText(cfg.get("dnsServer", ""))
        self.le_stun_server.setText(cfg.get("natHoleStunServer", ""))
        self.chk_login_fail_exit.setChecked(cfg.get("loginFailExit", True))
        self.te_start.setPlainText(UiHelpers.text_from_list(cfg.get("start")))
        self.te_includes.setPlainText(UiHelpers.text_from_list(cfg.get("includes")))
        self.sb_udp_packet_size.setValue(int(cfg.get("udpPacketSize", 0) or 0))
        self.le_store_path.setText((cfg.get("store") or {}).get("path", "") if isinstance(cfg.get("store"), dict) else "")
        self.le_virtual_net.setText((cfg.get("virtualNet") or {}).get("address", "") if isinstance(cfg.get("virtualNet"), dict) else "")
        self.te_root_metadatas.setPlainText(UiHelpers.dict_to_kv_text(cfg.get("metadatas")))
        self.te_root_feature_gates.setPlainText(UiHelpers.dict_to_kv_text(cfg.get("featureGates")))

        auth = cfg.get("auth", {}) if isinstance(cfg.get("auth"), dict) else {}
        self.cb_auth_method.setCurrentText(auth.get("method", "token"))
        self.le_token.setText(auth.get("token", ""))
        self.te_auth_token_source.setPlainText(UiHelpers.obj_to_json_text(auth.get("tokenSource")))
        scopes = set(auth.get("additionalScopes", []))
        self.chk_scope_heartbeats.setChecked("HeartBeats" in scopes)
        self.chk_scope_new_work_conns.setChecked("NewWorkConns" in scopes)
        oidc = auth.get("oidc", {}) if isinstance(auth.get("oidc"), dict) else {}
        self.le_oidc_client_id.setText(oidc.get("clientID", ""))
        self.le_oidc_client_secret.setText(oidc.get("clientSecret", ""))
        self.le_oidc_audience.setText(oidc.get("audience", ""))
        self.le_oidc_scope.setText(oidc.get("scope", ""))
        self.le_oidc_token_endpoint.setText(oidc.get("tokenEndpointURL", ""))
        self.te_oidc_additional_params.setPlainText(UiHelpers.dict_to_kv_text(oidc.get("additionalEndpointParams")))
        self.le_oidc_trusted_ca.setText(oidc.get("trustedCaFile", ""))
        self.chk_oidc_skip_verify.setChecked(bool(oidc.get("insecureSkipVerify", False)))
        self.le_oidc_proxy_url.setText(oidc.get("proxyURL", ""))
        self.te_oidc_token_source.setPlainText(UiHelpers.obj_to_json_text(oidc.get("tokenSource")))

        transport = cfg.get("transport", {}) if isinstance(cfg.get("transport"), dict) else {}
        self.cb_transport_protocol.setCurrentText(transport.get("protocol", "tcp"))
        self.sb_dial_server_timeout.setValue(int(transport.get("dialServerTimeout", 0) or 0))
        self.sb_dial_server_keepalive.setValue(int(transport.get("dialServerKeepalive", 0) or 0))
        self.le_connect_server_local_ip.setText(transport.get("connectServerLocalIP", ""))
        self.le_transport_proxy_url.setText(transport.get("proxyURL", ""))
        self.sb_pool_count.setValue(int(transport.get("poolCount", 0) or 0))
        self.chk_tcp_mux.setChecked(bool(transport.get("tcpMux", True)))
        self.sb_tcp_mux_keepalive.setValue(int(transport.get("tcpMuxKeepaliveInterval", 0) or 0))
        self.sb_heartbeat_interval.setValue(int(transport.get("heartbeatInterval", -1) if transport.get("heartbeatInterval") is not None else -1))
        self.sb_heartbeat_timeout.setValue(int(transport.get("heartbeatTimeout", 0) or 0))
        tls = transport.get("tls", {}) if isinstance(transport.get("tls"), dict) else {}
        self.chk_tls_enable.setChecked(bool(tls.get("enable", True)))
        self.chk_tls_disable_custom_byte.setChecked(bool(tls.get("disableCustomTLSFirstByte", True)))
        self.le_tls_cert_file.setText(tls.get("certFile", ""))
        self.le_tls_key_file.setText(tls.get("keyFile", ""))
        self.le_tls_trusted_ca.setText(tls.get("trustedCaFile", ""))
        self.le_tls_server_name.setText(tls.get("serverName", ""))
        quic = transport.get("quic", {}) if isinstance(transport.get("quic"), dict) else {}
        self.sb_quic_keepalive.setValue(int(quic.get("keepalivePeriod", 0) or 0))
        self.sb_quic_idle_timeout.setValue(int(quic.get("maxIdleTimeout", 0) or 0))
        self.sb_quic_max_streams.setValue(int(quic.get("maxIncomingStreams", 0) or 0))

        web_server = cfg.get("webServer", {}) if isinstance(cfg.get("webServer"), dict) else {}
        self.le_web_addr.setText(web_server.get("addr", ""))
        self.sb_web_port.setValue(int(web_server.get("port", 0) or 0))
        self.le_web_user.setText(web_server.get("user", ""))
        self.le_web_password.setText(web_server.get("password", ""))
        self.le_web_assets_dir.setText(web_server.get("assetsDir", ""))
        self.chk_web_pprof.setChecked(bool(web_server.get("pprofEnable", False)))
        web_tls = web_server.get("tls", {}) if isinstance(web_server.get("tls"), dict) else {}
        self.le_web_tls_cert.setText(web_tls.get("certFile", ""))
        self.le_web_tls_key.setText(web_tls.get("keyFile", ""))
        self.le_web_tls_ca.setText(web_tls.get("trustedCaFile", ""))
        self.le_web_tls_server_name.setText(web_tls.get("serverName", ""))

        log_cfg = cfg.get("log", {}) if isinstance(cfg.get("log"), dict) else {}
        self.le_log_to.setText(log_cfg.get("to", "console"))
        self.cb_log_level.setCurrentText(log_cfg.get("level", "info"))
        self.sb_log_max_days.setValue(int(log_cfg.get("maxDays", 0) or 0))
        self.chk_log_disable_color.setChecked(bool(log_cfg.get("disablePrintColor", False)))

        root_extras = {k: v for k, v in cfg.items() if k not in ROOT_KNOWN_KEYS}
        self.te_root_extras.setPlainText(UiHelpers.obj_to_json_text(root_extras))

        self.proxies = deep_copy(cfg.get("proxies", []))
        self.visitors = deep_copy(cfg.get("visitors", []))
        self._refresh_proxy_table()
        self._refresh_visitor_table()

        if self.proxies:
            self.table_proxies.selectRow(0)
            self._load_proxy_to_editor(0)
        else:
            self._clear_proxy_editor()
        if self.visitors:
            self.table_visitors.selectRow(0)
            self._load_visitor_to_editor(0)
        else:
            self._clear_visitor_editor()

    def _build_config_from_ui(self, skip_current_sync: bool = False) -> Dict[str, Any]:
        if not skip_current_sync:
            if not self._apply_current_proxy(silent=True):
                raise UiError("当前代理编辑区存在无效配置，请先修正")
            if not self._apply_current_visitor(silent=True):
                raise UiError("当前 Visitor 编辑区存在无效配置，请先修正")

        root_extra = UiHelpers.json_text_to_obj(self.te_root_extras.toPlainText(), "根级 extra JSON") or {}

        cfg: Dict[str, Any] = deep_copy(root_extra)
        cfg["serverAddr"] = self.le_server_addr.text().strip()
        cfg["serverPort"] = self.sb_server_port.value()
        cfg["user"] = self.le_user.text().strip() or None
        cfg["dnsServer"] = self.le_dns_server.text().strip() or None
        cfg["natHoleStunServer"] = self.le_stun_server.text().strip() or None
        cfg["loginFailExit"] = self.chk_login_fail_exit.isChecked()
        cfg["start"] = UiHelpers.list_from_text(self.te_start.toPlainText())
        cfg["includes"] = UiHelpers.list_from_text(self.te_includes.toPlainText())
        cfg["udpPacketSize"] = UiHelpers.spin_value_or_none(self.sb_udp_packet_size)
        cfg["metadatas"] = UiHelpers.kv_text_to_dict(self.te_root_metadatas.toPlainText())
        cfg["featureGates"] = UiHelpers.kv_text_to_dict(self.te_root_feature_gates.toPlainText(), bool_values=True)

        store_path = self.le_store_path.text().strip()
        cfg["store"] = {"path": store_path} if store_path else None
        virtual_net_address = self.le_virtual_net.text().strip()
        cfg["virtualNet"] = {"address": virtual_net_address} if virtual_net_address else None

        auth: Dict[str, Any] = {
            "method": self.cb_auth_method.currentText(),
            "token": self.le_token.text().strip() or None,
            "tokenSource": UiHelpers.json_text_to_obj(self.te_auth_token_source.toPlainText(), "auth.tokenSource JSON"),
        }
        scopes = []
        if self.chk_scope_heartbeats.isChecked():
            scopes.append("HeartBeats")
        if self.chk_scope_new_work_conns.isChecked():
            scopes.append("NewWorkConns")
        auth["additionalScopes"] = scopes

        oidc: Dict[str, Any] = {
            "clientID": self.le_oidc_client_id.text().strip() or None,
            "clientSecret": self.le_oidc_client_secret.text().strip() or None,
            "audience": self.le_oidc_audience.text().strip() or None,
            "scope": self.le_oidc_scope.text().strip() or None,
            "tokenEndpointURL": self.le_oidc_token_endpoint.text().strip() or None,
            "additionalEndpointParams": UiHelpers.kv_text_to_dict(self.te_oidc_additional_params.toPlainText()),
            "trustedCaFile": self.le_oidc_trusted_ca.text().strip() or None,
            "insecureSkipVerify": self.chk_oidc_skip_verify.isChecked(),
            "proxyURL": self.le_oidc_proxy_url.text().strip() or None,
            "tokenSource": UiHelpers.json_text_to_obj(self.te_oidc_token_source.toPlainText(), "auth.oidc.tokenSource JSON"),
        }
        auth["oidc"] = compact(oidc)
        if auth.get("token") and auth.get("tokenSource"):
            raise UiError("auth.token 与 auth.tokenSource 互斥，请二选一")
        cfg["auth"] = compact(auth)

        transport: Dict[str, Any] = {
            "protocol": self.cb_transport_protocol.currentText(),
            "dialServerTimeout": UiHelpers.spin_value_or_none(self.sb_dial_server_timeout),
            "dialServerKeepalive": UiHelpers.spin_value_or_none(self.sb_dial_server_keepalive),
            "connectServerLocalIP": self.le_connect_server_local_ip.text().strip() or None,
            "proxyURL": self.le_transport_proxy_url.text().strip() or None,
            "poolCount": UiHelpers.spin_value_or_none(self.sb_pool_count),
            "tcpMux": self.chk_tcp_mux.isChecked(),
            "tcpMuxKeepaliveInterval": UiHelpers.spin_value_or_none(self.sb_tcp_mux_keepalive),
            "heartbeatInterval": None if self.sb_heartbeat_interval.value() == 0 else self.sb_heartbeat_interval.value(),
            "heartbeatTimeout": UiHelpers.spin_value_or_none(self.sb_heartbeat_timeout),
            "tls": {
                "enable": self.chk_tls_enable.isChecked(),
                "disableCustomTLSFirstByte": self.chk_tls_disable_custom_byte.isChecked(),
                "certFile": self.le_tls_cert_file.text().strip() or None,
                "keyFile": self.le_tls_key_file.text().strip() or None,
                "trustedCaFile": self.le_tls_trusted_ca.text().strip() or None,
                "serverName": self.le_tls_server_name.text().strip() or None,
            },
            "quic": {
                "keepalivePeriod": UiHelpers.spin_value_or_none(self.sb_quic_keepalive),
                "maxIdleTimeout": UiHelpers.spin_value_or_none(self.sb_quic_idle_timeout),
                "maxIncomingStreams": UiHelpers.spin_value_or_none(self.sb_quic_max_streams),
            },
        }
        cfg["transport"] = compact(transport)

        web_tls = {
            "certFile": self.le_web_tls_cert.text().strip() or None,
            "keyFile": self.le_web_tls_key.text().strip() or None,
            "trustedCaFile": self.le_web_tls_ca.text().strip() or None,
            "serverName": self.le_web_tls_server_name.text().strip() or None,
        }
        web_port = UiHelpers.spin_value_or_none(self.sb_web_port)
        cfg["webServer"] = compact(
            {
                "addr": self.le_web_addr.text().strip() or None,
                "port": web_port,
                "user": self.le_web_user.text().strip() or None,
                "password": self.le_web_password.text().strip() or None,
                "assetsDir": self.le_web_assets_dir.text().strip() or None,
                "pprofEnable": self.chk_web_pprof.isChecked(),
                "tls": web_tls,
            }
        )

        cfg["log"] = compact(
            {
                "to": self.le_log_to.text().strip() or None,
                "level": self.cb_log_level.currentText(),
                "maxDays": UiHelpers.spin_value_or_none(self.sb_log_max_days),
                "disablePrintColor": self.chk_log_disable_color.isChecked(),
            }
        )

        cfg["proxies"] = deep_copy(self.proxies)
        cfg["visitors"] = deep_copy(self.visitors)

        if not cfg.get("serverAddr"):
            raise UiError("serverAddr 不能为空")
        return compact(cfg)

    def _save_config(self, show_message: bool = True) -> bool:
        try:
            config = self._build_config_from_ui()
        except UiError as exc:
            if show_message:
                QMessageBox.warning(self, "配置错误", str(exc))
            return False
        if self.cfg_mgr.save_config(config):
            self.current_config = config
            self.proc_mgr.set_paths(config_path=self.cfg_mgr.config_path)
            self._update_preview(config)
            if show_message:
                QMessageBox.information(self, "成功", f"配置已保存到\n{self.cfg_mgr.config_path}")
            return True
        if show_message:
            QMessageBox.warning(self, "错误", "配置保存失败")
        return False

    def _reload_from_file(self) -> None:
        config = self.cfg_mgr.load_config()
        self._load_config_to_ui(config)
        self._update_preview(config)
        self._append_log(f"[INFO] 已从文件重新加载配置: {self.cfg_mgr.config_path}")

    def _save_and_start(self) -> None:
        if self._save_config(show_message=False):
            self.proc_mgr.start()
        else:
            QMessageBox.warning(self, "配置错误", "启动前保存配置失败，请先修正配置")

    def _update_preview(self, config: Optional[Dict[str, Any]] = None) -> None:
        try:
            config = config or self._build_config_from_ui(skip_current_sync=self._syncing_current_item)
            preview = self.cfg_mgr.dumps(config)
        except Exception as exc:
            preview = f"# 无法生成预览\n# {exc}"
        self.txt_preview.setPlainText(preview)

    # ---------- proxies ----------
    def _new_proxy_template(self) -> Dict[str, Any]:
        return {
            "name": f"proxy_{len(self.proxies) + 1}",
            "type": "tcp",
            "enabled": True,
            "localIP": "127.0.0.1",
            "localPort": 8080,
            "remotePort": 8080,
        }

    def _add_proxy(self) -> None:
        self._apply_current_proxy(silent=True)
        self.proxies.append(self._new_proxy_template())
        self._refresh_proxy_table()
        new_row = len(self.proxies) - 1
        self.table_proxies.selectRow(new_row)
        self._load_proxy_to_editor(new_row)

    def _copy_proxy(self) -> None:
        idx = self.current_proxy_index
        if idx < 0 or idx >= len(self.proxies):
            QMessageBox.information(self, "提示", "请先选择一个代理")
            return
        self._apply_current_proxy(silent=True)
        cloned = deep_copy(self.proxies[idx])
        cloned["name"] = f"{cloned.get('name', 'proxy')}_copy"
        self.proxies.append(cloned)
        self._refresh_proxy_table()
        new_row = len(self.proxies) - 1
        self.table_proxies.selectRow(new_row)
        self._load_proxy_to_editor(new_row)

    def _delete_proxy(self) -> None:
        idx = self.current_proxy_index
        if idx < 0 or idx >= len(self.proxies):
            return
        del self.proxies[idx]
        self.current_proxy_index = -1
        self._refresh_proxy_table()
        if self.proxies:
            row = min(idx, len(self.proxies) - 1)
            self.table_proxies.selectRow(row)
            self._load_proxy_to_editor(row)
        else:
            self._clear_proxy_editor()
        self._update_preview()

    def _apply_current_proxy(self, silent: bool = False) -> bool:
        if self._loading_proxy_editor:
            return True
        idx = self.current_proxy_index
        if idx < 0:
            return True
        try:
            self._syncing_current_item = True
            proxy = self._collect_proxy_from_editor()
            self.proxies[idx] = proxy
            self._refresh_proxy_table(select_row=idx)
            self._update_preview(self._build_config_from_ui(skip_current_sync=True))
            return True
        except UiError as exc:
            if not silent:
                QMessageBox.warning(self, "代理配置错误", str(exc))
            return False
        finally:
            self._syncing_current_item = False

    def _collect_proxy_from_editor(self) -> Dict[str, Any]:
        name = self.le_proxy_name.text().strip()
        if not name:
            raise UiError("代理 name 不能为空")
        proxy_type = self.cb_proxy_type.currentText().strip()
        if not proxy_type:
            raise UiError("代理 type 不能为空")

        extra = UiHelpers.json_text_to_obj(self.te_proxy_extra_json.toPlainText(), "proxy extra JSON") or {}
        plugin = UiHelpers.json_text_to_obj(self.te_proxy_plugin_json.toPlainText(), "proxy plugin JSON")

        proxy: Dict[str, Any] = deep_copy(extra)
        proxy.update(
            {
                "name": name,
                "type": proxy_type,
                "enabled": self.chk_proxy_enabled.isChecked(),
                "localIP": self.le_proxy_local_ip.text().strip() or None,
                "localPort": UiHelpers.spin_value_or_none(self.sb_proxy_local_port),
                "remotePort": UiHelpers.spin_value_or_none(self.sb_proxy_remote_port),
                "customDomains": UiHelpers.list_from_text(self.te_proxy_custom_domains.toPlainText()),
                "subdomain": self.le_proxy_subdomain.text().strip() or None,
                "locations": UiHelpers.list_from_text(self.te_proxy_locations.toPlainText()),
                "httpUser": self.le_proxy_http_user.text().strip() or None,
                "httpPassword": self.le_proxy_http_password.text().strip() or None,
                "hostHeaderRewrite": self.le_proxy_host_header_rewrite.text().strip() or None,
                "routeByHTTPUser": self.le_proxy_route_by_http_user.text().strip() or None,
                "multiplexer": self.cb_proxy_multiplexer.currentText().strip() or None,
                "secretKey": self.le_proxy_secret_key.text().strip() or None,
                "allowUsers": UiHelpers.list_from_text(self.te_proxy_allow_users.toPlainText()),
                "annotations": UiHelpers.kv_text_to_dict(self.te_proxy_annotations.toPlainText()),
                "metadatas": UiHelpers.kv_text_to_dict(self.te_proxy_metadatas.toPlainText()),
                "transport": {
                    "useEncryption": self.chk_proxy_use_encryption.isChecked(),
                    "useCompression": self.chk_proxy_use_compression.isChecked(),
                    "bandwidthLimit": self.le_proxy_bandwidth_limit.text().strip() or None,
                    "bandwidthLimitMode": self.cb_proxy_bandwidth_mode.currentText().strip() or None,
                    "proxyProtocolVersion": self.cb_proxy_proxy_protocol.currentText().strip() or None,
                },
                "loadBalancer": {
                    "group": self.le_proxy_lb_group.text().strip() or None,
                    "groupKey": self.le_proxy_lb_group_key.text().strip() or None,
                },
                "healthCheck": {
                    "type": self.cb_proxy_hc_type.currentText().strip() or None,
                    "timeoutSeconds": UiHelpers.spin_value_or_none(self.sb_proxy_hc_timeout),
                    "maxFailed": UiHelpers.spin_value_or_none(self.sb_proxy_hc_max_failed),
                    "intervalSeconds": UiHelpers.spin_value_or_none(self.sb_proxy_hc_interval),
                    "path": self.le_proxy_hc_path.text().strip() or None,
                    "httpHeaders": self._headers_list_from_kv(self.te_proxy_hc_headers.toPlainText()),
                },
                "requestHeaders": self._header_operations_from_text(self.te_proxy_request_headers.toPlainText()),
                "responseHeaders": self._header_operations_from_text(self.te_proxy_response_headers.toPlainText()),
                "plugin": plugin,
            }
        )
        proxy = compact(proxy)

        proxy_has_plugin = isinstance(proxy.get("plugin"), dict) and bool(proxy.get("plugin"))
        if not proxy_has_plugin and proxy_type in {"tcp", "udp", "http", "https", "tcpmux", "stcp", "sudp", "xtcp"}:
            if not proxy.get("localPort"):
                raise UiError(f"代理 {name}: 未配置 plugin 时 localPort 不能为空")
        if proxy_type in {"tcp", "udp"} and not proxy.get("remotePort"):
            raise UiError(f"代理 {name}: {proxy_type} 类型需要 remotePort")
        if proxy_type in {"http", "https", "tcpmux"} and not (proxy.get("customDomains") or proxy.get("subdomain")):
            raise UiError(f"代理 {name}: {proxy_type} 类型至少需要 customDomains 或 subdomain")
        return proxy

    def _on_proxy_selection_changed(self) -> None:
        selected = self.table_proxies.selectionModel().selectedRows()
        new_index = selected[0].row() if selected else -1
        if new_index == self.current_proxy_index:
            return
        old_index = self.current_proxy_index
        if old_index >= 0 and not self._apply_current_proxy(silent=True):
            self.table_proxies.selectRow(old_index)
            return
        if new_index >= 0:
            self._load_proxy_to_editor(new_index)
        else:
            self._clear_proxy_editor()

    def _load_proxy_to_editor(self, index: int) -> None:
        if index < 0 or index >= len(self.proxies):
            self._clear_proxy_editor()
            return
        proxy = deep_copy(self.proxies[index])
        self._loading_proxy_editor = True
        self.current_proxy_index = index
        self.le_proxy_name.setText(proxy.get("name", ""))
        self.cb_proxy_type.setCurrentText(proxy.get("type", "tcp"))
        self.chk_proxy_enabled.setChecked(bool(proxy.get("enabled", True)))
        self.le_proxy_local_ip.setText(proxy.get("localIP", "127.0.0.1"))
        self.sb_proxy_local_port.setValue(int(proxy.get("localPort", 0) or 0))
        self.sb_proxy_remote_port.setValue(int(proxy.get("remotePort", 0) or 0))
        self.te_proxy_custom_domains.setPlainText(UiHelpers.text_from_list(proxy.get("customDomains")))
        self.le_proxy_subdomain.setText(proxy.get("subdomain", ""))
        self.te_proxy_locations.setPlainText(UiHelpers.text_from_list(proxy.get("locations")))
        self.le_proxy_http_user.setText(proxy.get("httpUser", ""))
        self.le_proxy_http_password.setText(proxy.get("httpPassword", ""))
        self.le_proxy_host_header_rewrite.setText(proxy.get("hostHeaderRewrite", ""))
        self.le_proxy_route_by_http_user.setText(proxy.get("routeByHTTPUser", ""))
        self.cb_proxy_multiplexer.setCurrentText(proxy.get("multiplexer", ""))
        self.le_proxy_secret_key.setText(proxy.get("secretKey", ""))
        self.te_proxy_allow_users.setPlainText(UiHelpers.text_from_list(proxy.get("allowUsers")))
        transport = proxy.get("transport", {}) if isinstance(proxy.get("transport"), dict) else {}
        self.chk_proxy_use_encryption.setChecked(bool(transport.get("useEncryption", False)))
        self.chk_proxy_use_compression.setChecked(bool(transport.get("useCompression", False)))
        self.le_proxy_bandwidth_limit.setText(transport.get("bandwidthLimit", ""))
        self.cb_proxy_bandwidth_mode.setCurrentText(transport.get("bandwidthLimitMode", ""))
        self.cb_proxy_proxy_protocol.setCurrentText(transport.get("proxyProtocolVersion", ""))
        self.te_proxy_plugin_json.setPlainText(UiHelpers.obj_to_json_text(proxy.get("plugin")))
        load_balancer = proxy.get("loadBalancer", {}) if isinstance(proxy.get("loadBalancer"), dict) else {}
        self.le_proxy_lb_group.setText(load_balancer.get("group", ""))
        self.le_proxy_lb_group_key.setText(load_balancer.get("groupKey", ""))
        health_check = proxy.get("healthCheck", {}) if isinstance(proxy.get("healthCheck"), dict) else {}
        self.cb_proxy_hc_type.setCurrentText(health_check.get("type", ""))
        self.sb_proxy_hc_timeout.setValue(int(health_check.get("timeoutSeconds", 0) or 0))
        self.sb_proxy_hc_max_failed.setValue(int(health_check.get("maxFailed", 0) or 0))
        self.sb_proxy_hc_interval.setValue(int(health_check.get("intervalSeconds", 0) or 0))
        self.le_proxy_hc_path.setText(health_check.get("path", ""))
        self.te_proxy_hc_headers.setPlainText(self._headers_list_to_text(health_check.get("httpHeaders")))
        self.te_proxy_annotations.setPlainText(UiHelpers.dict_to_kv_text(proxy.get("annotations")))
        self.te_proxy_metadatas.setPlainText(UiHelpers.dict_to_kv_text(proxy.get("metadatas")))
        self.te_proxy_request_headers.setPlainText(UiHelpers.dict_to_kv_text((proxy.get("requestHeaders") or {}).get("set") if isinstance(proxy.get("requestHeaders"), dict) else {}))
        self.te_proxy_response_headers.setPlainText(UiHelpers.dict_to_kv_text((proxy.get("responseHeaders") or {}).get("set") if isinstance(proxy.get("responseHeaders"), dict) else {}))
        extras = {k: v for k, v in proxy.items() if k not in PROXY_KNOWN_KEYS}
        self.te_proxy_extra_json.setPlainText(UiHelpers.obj_to_json_text(extras))
        self._loading_proxy_editor = False

    def _clear_proxy_editor(self) -> None:
        self._loading_proxy_editor = True
        self.current_proxy_index = -1
        for widget in [
            self.le_proxy_name,
            self.le_proxy_local_ip,
            self.le_proxy_subdomain,
            self.le_proxy_http_user,
            self.le_proxy_http_password,
            self.le_proxy_host_header_rewrite,
            self.le_proxy_route_by_http_user,
            self.le_proxy_secret_key,
            self.le_proxy_bandwidth_limit,
            self.le_proxy_lb_group,
            self.le_proxy_lb_group_key,
            self.le_proxy_hc_path,
        ]:
            widget.clear()
        for widget in [
            self.te_proxy_custom_domains,
            self.te_proxy_locations,
            self.te_proxy_allow_users,
            self.te_proxy_plugin_json,
            self.te_proxy_hc_headers,
            self.te_proxy_annotations,
            self.te_proxy_metadatas,
            self.te_proxy_request_headers,
            self.te_proxy_response_headers,
            self.te_proxy_extra_json,
        ]:
            widget.setPlainText("")
        self.cb_proxy_type.setCurrentText("tcp")
        self.cb_proxy_multiplexer.setCurrentText("")
        self.cb_proxy_bandwidth_mode.setCurrentText("")
        self.cb_proxy_proxy_protocol.setCurrentText("")
        self.cb_proxy_hc_type.setCurrentText("")
        self.chk_proxy_enabled.setChecked(True)
        self.chk_proxy_use_encryption.setChecked(False)
        self.chk_proxy_use_compression.setChecked(False)
        self.sb_proxy_local_port.setValue(0)
        self.sb_proxy_remote_port.setValue(0)
        self.sb_proxy_hc_timeout.setValue(0)
        self.sb_proxy_hc_max_failed.setValue(0)
        self.sb_proxy_hc_interval.setValue(0)
        self._loading_proxy_editor = False

    def _refresh_proxy_table(self, select_row: Optional[int] = None) -> None:
        self.table_proxies.setRowCount(0)
        for row, proxy in enumerate(self.proxies):
            self.table_proxies.insertRow(row)
            summary = self._proxy_summary(proxy)
            values = [
                proxy.get("name", ""),
                proxy.get("type", ""),
                "是" if proxy.get("enabled", True) else "否",
                summary,
            ]
            for col, value in enumerate(values):
                self.table_proxies.setItem(row, col, QTableWidgetItem(str(value)))
        if select_row is not None and 0 <= select_row < self.table_proxies.rowCount():
            self.table_proxies.blockSignals(True)
            self.table_proxies.selectRow(select_row)
            self.table_proxies.blockSignals(False)

    def _proxy_summary(self, proxy: Dict[str, Any]) -> str:
        proxy_type = proxy.get("type", "")
        if proxy_type in {"tcp", "udp"}:
            return f"{proxy.get('localIP', '127.0.0.1')}:{proxy.get('localPort', '')} -> remotePort {proxy.get('remotePort', '')}"
        if proxy_type in {"http", "https", "tcpmux"}:
            domains = proxy.get("customDomains") or []
            subdomain = proxy.get("subdomain") or ""
            domain_part = ",".join(domains) if domains else subdomain
            return f"{proxy.get('localIP', '127.0.0.1')}:{proxy.get('localPort', '')} -> {domain_part or '未设置域名'}"
        if proxy_type in {"stcp", "sudp", "xtcp"}:
            return f"{proxy.get('localIP', '127.0.0.1')}:{proxy.get('localPort', '')} secretKey={'***' if proxy.get('secretKey') else '空'}"
        return ""

    # ---------- visitors ----------
    def _new_visitor_template(self) -> Dict[str, Any]:
        return {
            "name": f"visitor_{len(self.visitors) + 1}",
            "type": "stcp",
            "enabled": True,
            "bindAddr": "127.0.0.1",
            "bindPort": -1,
            "serverName": "",
        }

    def _add_visitor(self) -> None:
        self._apply_current_visitor(silent=True)
        self.visitors.append(self._new_visitor_template())
        self._refresh_visitor_table()
        new_row = len(self.visitors) - 1
        self.table_visitors.selectRow(new_row)
        self._load_visitor_to_editor(new_row)

    def _copy_visitor(self) -> None:
        idx = self.current_visitor_index
        if idx < 0 or idx >= len(self.visitors):
            QMessageBox.information(self, "提示", "请先选择一个 Visitor")
            return
        self._apply_current_visitor(silent=True)
        cloned = deep_copy(self.visitors[idx])
        cloned["name"] = f"{cloned.get('name', 'visitor')}_copy"
        self.visitors.append(cloned)
        self._refresh_visitor_table()
        new_row = len(self.visitors) - 1
        self.table_visitors.selectRow(new_row)
        self._load_visitor_to_editor(new_row)

    def _delete_visitor(self) -> None:
        idx = self.current_visitor_index
        if idx < 0 or idx >= len(self.visitors):
            return
        del self.visitors[idx]
        self.current_visitor_index = -1
        self._refresh_visitor_table()
        if self.visitors:
            row = min(idx, len(self.visitors) - 1)
            self.table_visitors.selectRow(row)
            self._load_visitor_to_editor(row)
        else:
            self._clear_visitor_editor()
        self._update_preview()

    def _apply_current_visitor(self, silent: bool = False) -> bool:
        if self._loading_visitor_editor:
            return True
        idx = self.current_visitor_index
        if idx < 0:
            return True
        try:
            self._syncing_current_item = True
            visitor = self._collect_visitor_from_editor()
            self.visitors[idx] = visitor
            self._refresh_visitor_table(select_row=idx)
            self._update_preview(self._build_config_from_ui(skip_current_sync=True))
            return True
        except UiError as exc:
            if not silent:
                QMessageBox.warning(self, "Visitor 配置错误", str(exc))
            return False
        finally:
            self._syncing_current_item = False

    def _collect_visitor_from_editor(self) -> Dict[str, Any]:
        name = self.le_visitor_name.text().strip()
        if not name:
            raise UiError("Visitor name 不能为空")
        visitor_type = self.cb_visitor_type.currentText().strip()
        if not visitor_type:
            raise UiError("Visitor type 不能为空")
        server_name = self.le_visitor_server_name.text().strip()
        if not server_name:
            raise UiError("Visitor serverName 不能为空")

        extra = UiHelpers.json_text_to_obj(self.te_visitor_extra_json.toPlainText(), "visitor extra JSON") or {}
        plugin = UiHelpers.json_text_to_obj(self.te_visitor_plugin_json.toPlainText(), "visitor plugin JSON")

        visitor: Dict[str, Any] = deep_copy(extra)
        visitor.update(
            {
                "name": name,
                "type": visitor_type,
                "enabled": self.chk_visitor_enabled.isChecked(),
                "secretKey": self.le_visitor_secret_key.text().strip() or None,
                "serverUser": self.le_visitor_server_user.text().strip() or None,
                "serverName": server_name,
                "bindAddr": self.le_visitor_bind_addr.text().strip() or None,
                "bindPort": self.sb_visitor_bind_port.value(),
                "plugin": plugin,
                "transport": {
                    "useEncryption": self.chk_visitor_use_encryption.isChecked(),
                    "useCompression": self.chk_visitor_use_compression.isChecked(),
                },
                "protocol": self.cb_visitor_protocol.currentText().strip() or None,
                "keepTunnelOpen": self.chk_visitor_keep_tunnel_open.isChecked(),
                "maxRetriesAnHour": UiHelpers.spin_value_or_none(self.sb_visitor_max_retries),
                "minRetryInterval": UiHelpers.spin_value_or_none(self.sb_visitor_min_retry),
                "fallbackTo": self.le_visitor_fallback_to.text().strip() or None,
                "fallbackTimeoutMs": UiHelpers.spin_value_or_none(self.sb_visitor_fallback_timeout),
                "natTraversal": {
                    "disableAssistedAddrs": self.chk_visitor_disable_assisted_addrs.isChecked(),
                },
            }
        )
        if visitor["bindPort"] == 0:
            raise UiError(f"Visitor {name}: bindPort 不能为 0，可设置为 -1 或具体端口")
        return compact(visitor)

    def _on_visitor_selection_changed(self) -> None:
        selected = self.table_visitors.selectionModel().selectedRows()
        new_index = selected[0].row() if selected else -1
        if new_index == self.current_visitor_index:
            return
        old_index = self.current_visitor_index
        if old_index >= 0 and not self._apply_current_visitor(silent=True):
            self.table_visitors.selectRow(old_index)
            return
        if new_index >= 0:
            self._load_visitor_to_editor(new_index)
        else:
            self._clear_visitor_editor()

    def _load_visitor_to_editor(self, index: int) -> None:
        if index < 0 or index >= len(self.visitors):
            self._clear_visitor_editor()
            return
        visitor = deep_copy(self.visitors[index])
        self._loading_visitor_editor = True
        self.current_visitor_index = index
        self.le_visitor_name.setText(visitor.get("name", ""))
        self.cb_visitor_type.setCurrentText(visitor.get("type", "stcp"))
        self.chk_visitor_enabled.setChecked(bool(visitor.get("enabled", True)))
        self.le_visitor_secret_key.setText(visitor.get("secretKey", ""))
        self.le_visitor_server_user.setText(visitor.get("serverUser", ""))
        self.le_visitor_server_name.setText(visitor.get("serverName", ""))
        self.le_visitor_bind_addr.setText(visitor.get("bindAddr", ""))
        self.sb_visitor_bind_port.setValue(int(visitor.get("bindPort", -1)))
        transport = visitor.get("transport", {}) if isinstance(visitor.get("transport"), dict) else {}
        self.chk_visitor_use_encryption.setChecked(bool(transport.get("useEncryption", False)))
        self.chk_visitor_use_compression.setChecked(bool(transport.get("useCompression", False)))
        self.cb_visitor_protocol.setCurrentText(visitor.get("protocol", ""))
        self.chk_visitor_keep_tunnel_open.setChecked(bool(visitor.get("keepTunnelOpen", False)))
        self.sb_visitor_max_retries.setValue(int(visitor.get("maxRetriesAnHour", 0) or 0))
        self.sb_visitor_min_retry.setValue(int(visitor.get("minRetryInterval", 0) or 0))
        self.le_visitor_fallback_to.setText(visitor.get("fallbackTo", ""))
        self.sb_visitor_fallback_timeout.setValue(int(visitor.get("fallbackTimeoutMs", 0) or 0))
        nat = visitor.get("natTraversal", {}) if isinstance(visitor.get("natTraversal"), dict) else {}
        self.chk_visitor_disable_assisted_addrs.setChecked(bool(nat.get("disableAssistedAddrs", False)))
        self.te_visitor_plugin_json.setPlainText(UiHelpers.obj_to_json_text(visitor.get("plugin")))
        extras = {k: v for k, v in visitor.items() if k not in VISITOR_KNOWN_KEYS}
        self.te_visitor_extra_json.setPlainText(UiHelpers.obj_to_json_text(extras))
        self._loading_visitor_editor = False

    def _clear_visitor_editor(self) -> None:
        self._loading_visitor_editor = True
        self.current_visitor_index = -1
        for widget in [
            self.le_visitor_name,
            self.le_visitor_secret_key,
            self.le_visitor_server_user,
            self.le_visitor_server_name,
            self.le_visitor_bind_addr,
            self.le_visitor_fallback_to,
        ]:
            widget.clear()
        self.cb_visitor_type.setCurrentText("stcp")
        self.chk_visitor_enabled.setChecked(True)
        self.chk_visitor_use_encryption.setChecked(False)
        self.chk_visitor_use_compression.setChecked(False)
        self.cb_visitor_protocol.setCurrentText("")
        self.chk_visitor_keep_tunnel_open.setChecked(False)
        self.sb_visitor_bind_port.setValue(-1)
        self.sb_visitor_max_retries.setValue(0)
        self.sb_visitor_min_retry.setValue(0)
        self.sb_visitor_fallback_timeout.setValue(0)
        self.chk_visitor_disable_assisted_addrs.setChecked(False)
        self.te_visitor_plugin_json.setPlainText("")
        self.te_visitor_extra_json.setPlainText("")
        self._loading_visitor_editor = False

    def _refresh_visitor_table(self, select_row: Optional[int] = None) -> None:
        self.table_visitors.setRowCount(0)
        for row, visitor in enumerate(self.visitors):
            self.table_visitors.insertRow(row)
            summary = f"{visitor.get('serverName', '')} @ {visitor.get('bindAddr', '127.0.0.1')}:{visitor.get('bindPort', '')}"
            values = [
                visitor.get("name", ""),
                visitor.get("type", ""),
                "是" if visitor.get("enabled", True) else "否",
                summary,
            ]
            for col, value in enumerate(values):
                self.table_visitors.setItem(row, col, QTableWidgetItem(str(value)))
        if select_row is not None and 0 <= select_row < self.table_visitors.rowCount():
            self.table_visitors.blockSignals(True)
            self.table_visitors.selectRow(select_row)
            self.table_visitors.blockSignals(False)

    # ---------- runtime ----------
    def _reload_runtime(self) -> None:
        if self._save_config(show_message=False):
            self.proc_mgr.reload_config()
        else:
            QMessageBox.warning(self, "配置错误", "热重载前保存配置失败，请先修正配置")

    def _show_runtime_status(self) -> None:
        if self._save_config(show_message=False):
            self.proc_mgr.show_status()
        else:
            QMessageBox.warning(self, "配置错误", "查看状态前保存配置失败，请先修正配置")

    def _update_status(self, is_running: bool) -> None:
        if is_running:
            self.lbl_status.setText("状态: 运行中")
            self.lbl_status.setStyleSheet("color: green; font-weight: bold;")
            self.btn_start.setEnabled(False)
            self.btn_stop.setEnabled(True)
        else:
            self.lbl_status.setText("状态: 未运行")
            self.lbl_status.setStyleSheet("color: red; font-weight: bold;")
            self.btn_start.setEnabled(True)
            self.btn_stop.setEnabled(False)

    def _append_log(self, text: str) -> None:
        if text:
            self.txt_log.append(text)

    # ---------- autostart ----------
    def _check_autostart(self) -> None:
        if winreg is None:
            self.chk_autostart.setEnabled(False)
            self.chk_autostart.setToolTip("当前系统不是 Windows，已禁用")
            return
        key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_READ)
            try:
                winreg.QueryValueEx(key, "FrpManagerEnhanced")
                self.chk_autostart.blockSignals(True)
                self.chk_autostart.setChecked(True)
                self.chk_autostart.blockSignals(False)
            except FileNotFoundError:
                pass
            finally:
                winreg.CloseKey(key)
        except Exception as exc:
            self._append_log(f"[WARN] 检查自启动失败: {exc}")

    def _toggle_autostart(self, state: int) -> None:
        if winreg is None:
            return
        key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        if getattr(sys, "frozen", False):
            cmd = f'"{os.path.abspath(sys.executable)}"'
        else:
            pythonw_path = os.path.join(os.path.dirname(sys.executable), "pythonw.exe")
            if not os.path.exists(pythonw_path):
                pythonw_path = sys.executable
            cmd = f'"{pythonw_path}" "{os.path.abspath(__file__)}"'
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_SET_VALUE)
            if state == Qt.Checked:
                winreg.SetValueEx(key, "FrpManagerEnhanced", 0, winreg.REG_SZ, cmd)
            else:
                try:
                    winreg.DeleteValue(key, "FrpManagerEnhanced")
                except FileNotFoundError:
                    pass
            winreg.CloseKey(key)
        except Exception as exc:
            QMessageBox.warning(self, "错误", f"无法操作注册表: {exc}")

    # ---------- header helpers ----------
    def _header_operations_from_text(self, text: str) -> Optional[Dict[str, Any]]:
        mapping = UiHelpers.kv_text_to_dict(text)
        if not mapping:
            return None
        return {"set": mapping}

    def _headers_list_from_kv(self, text: str) -> List[Dict[str, str]]:
        mapping = UiHelpers.kv_text_to_dict(text)
        return [{"name": k, "value": str(v)} for k, v in mapping.items()]

    def _headers_list_to_text(self, headers: Any) -> str:
        if not headers:
            return ""
        mapping = {}
        if isinstance(headers, list):
            for item in headers:
                if isinstance(item, dict) and item.get("name"):
                    mapping[str(item["name"])] = str(item.get("value", ""))
        return UiHelpers.dict_to_kv_text(mapping)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = FrpManagerWindow()
    window.show()
    sys.exit(app.exec_())
