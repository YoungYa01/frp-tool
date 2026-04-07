import os
from typing import Any, Dict, List, Optional

class ConfigManager:
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or os.path.join(app_dir(), "frpc.toml")
        self.default_config = {
            "serverAddr": "127.0.0.1",
            "serverPort": 7000,
            "loginFailExit": True,
            "transport": {
                "protocol": "tcp",
                "tcpMux": True,
                "tls": {
                    "enable": True,
                    "disableCustomTLSFirstByte": True,
                },
            },
            "auth": {"method": "token", "token": ""},
            "log": {"to": "console", "level": "info", "maxDays": 3},
            "proxies": [],
            "visitors": [],
        }

    def load_config(self) -> Dict[str, Any]:
        if not os.path.exists(self.config_path):
            return deep_copy(self.default_config)
        try:
            with open(self.config_path, "rb") as f:
                loaded = tomli.load(f)
            return self.normalize_config(loaded)
        except Exception as exc:
            print(f"Load config error: {exc}")
            return deep_copy(self.default_config)

    def save_config(self, config: Dict[str, Any]) -> bool:
        try:
            with open(self.config_path, "wb") as f:
                tomli_w.dump(compact(config), f)
            return True
        except Exception as exc:
            print(f"Save config error: {exc}")
            return False

    def dumps(self, config: Dict[str, Any]) -> str:
        buffer = io.BytesIO()
        tomli_w.dump(compact(config), buffer)
        return buffer.getvalue().decode("utf-8")

    def normalize_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        merged = deep_copy(self.default_config)
        merged.update(config or {})
        merged["auth"] = {**deep_copy(self.default_config["auth"]), **(config.get("auth", {}) if isinstance(config.get("auth"), dict) else {})}
        merged["log"] = {**deep_copy(self.default_config["log"]), **(config.get("log", {}) if isinstance(config.get("log"), dict) else {})}
        merged_transport = deep_copy(self.default_config["transport"])
        given_transport = config.get("transport", {}) if isinstance(config.get("transport"), dict) else {}
        merged_transport.update(given_transport)
        merged_transport["tls"] = {
            **deep_copy(self.default_config["transport"]["tls"]),
            **(given_transport.get("tls", {}) if isinstance(given_transport.get("tls"), dict) else {}),
        }
        merged["transport"] = merged_transport
        merged["proxies"] = list(config.get("proxies", [])) if isinstance(config.get("proxies"), list) else []
        merged["visitors"] = list(config.get("visitors", [])) if isinstance(config.get("visitors"), list) else []
        return merged
