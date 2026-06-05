import json
from datetime import date, datetime
from pathlib import Path


def salvar_staging(registros: list[dict], arquivo: Path) -> None:
    """Serializa lista de registros para JSON, convertendo date/datetime para ISO."""
    arquivo.parent.mkdir(parents=True, exist_ok=True)
    with open(arquivo, "w", encoding="utf-8") as f:
        json.dump(registros, f, default=_serializar, ensure_ascii=False, indent=2)


def carregar_staging(arquivo: Path) -> list[dict]:
    """Lê JSON de staging e converte strings ISO de volta para date/datetime."""
    with open(arquivo, "r", encoding="utf-8") as f:
        registros = json.load(f)
    return [_deserializar_registro(r) for r in registros]


def _serializar(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Tipo não serializável: {type(obj)}")


def _deserializar_registro(registro: dict) -> dict:
    """
    Percorre o registro e converte automaticamente strings no formato ISO
    (YYYY-MM-DD ou YYYY-MM-DDTHH:MM:SS) para os tipos Python corretos.
    """
    resultado = {}
    for chave, valor in registro.items():
        resultado[chave] = _tentar_converter_data(valor)
    return resultado


def _tentar_converter_data(valor):
    if not isinstance(valor, str):
        return valor
    for fmt, tem_hora in [
        ("%Y-%m-%dT%H:%M:%S.%f", True),
        ("%Y-%m-%dT%H:%M:%S", True),
        ("%Y-%m-%d", False),
    ]:
        try:
            parsed = datetime.strptime(valor, fmt)
            return parsed if tem_hora else parsed.date()
        except ValueError:
            continue
    return valor
