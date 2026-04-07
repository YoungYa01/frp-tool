from typing import Any, Dict, List, Optional
import json

from PyQt5.QtWidgets import QSpinBox


class UiHelpers:
    @staticmethod
    def list_from_text(text: str) -> List[str]:
        items: List[str] = []
        normalized = text.replace("\r", "\n")
        for chunk in normalized.split("\n"):
            for part in chunk.split(","):
                item = part.strip()
                if item:
                    items.append(item)
        return items

    @staticmethod
    def text_from_list(values: Optional[List[str]]) -> str:
        if not values:
            return ""
        return "\n".join(str(v) for v in values if str(v).strip())

    @staticmethod
    def parse_bool(text: str) -> bool:
        lowered = text.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
        raise UiError(f"无法解析布尔值: {text}")

    @staticmethod
    def kv_text_to_dict(text: str, bool_values: bool = False) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        stripped = text.strip()
        if not stripped:
            return result
        for idx, raw_line in enumerate(stripped.splitlines(), start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
            elif ":" in line:
                key, value = line.split(":", 1)
            else:
                raise UiError(f"第 {idx} 行格式错误，请使用 key=value 或 key: value")
            key = key.strip()
            value = value.strip()
            if not key:
                raise UiError(f"第 {idx} 行键名不能为空")
            result[key] = UiHelpers.parse_bool(value) if bool_values else value
        return result

    @staticmethod
    def dict_to_kv_text(data: Optional[Dict[str, Any]]) -> str:
        if not data:
            return ""
        lines = []
        for key, value in data.items():
            if isinstance(value, bool):
                value_str = "true" if value else "false"
            else:
                value_str = str(value)
            lines.append(f"{key}={value_str}")
        return "\n".join(lines)

    @staticmethod
    def json_text_to_obj(text: str, field_name: str, *, require_object: bool = True) -> Any:
        stripped = text.strip()
        if not stripped:
            return None
        try:
            data = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise UiError(f"{field_name} 不是合法 JSON: {exc}") from exc
        if require_object and not isinstance(data, dict):
            raise UiError(f"{field_name} 必须是 JSON 对象")
        return data

    @staticmethod
    def obj_to_json_text(data: Any) -> str:
        if data in (None, {}, []):
            return ""
        return json.dumps(data, ensure_ascii=False, indent=2)

    @staticmethod
    def spin_value_or_none(widget: QSpinBox, unset_values: Optional[List[int]] = None) -> Optional[int]:
        unset_values = unset_values or [0]
        value = widget.value()
        if value in unset_values:
            return None
        return value



class UiError(ValueError):
    pass
