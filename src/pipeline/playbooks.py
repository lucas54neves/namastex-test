from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Playbook:
    playbook_id: str
    summary: str
    risk_level: str
    safe_auto_apply: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "playbook_id": self.playbook_id,
            "summary": self.summary,
            "risk_level": self.risk_level,
            "safe_auto_apply": self.safe_auto_apply,
        }


PLAYBOOKS = {
    "rebuild_silver_from_bronze": Playbook(
        playbook_id="rebuild_silver_from_bronze",
        summary="Reconstrói a Silver a partir da Bronze usando a spec vigente.",
        risk_level="medium",
        safe_auto_apply=True,
    ),
    "rebuild_gold_from_silver": Playbook(
        playbook_id="rebuild_gold_from_silver",
        summary="Reconstrói a Gold a partir da Silver válida.",
        risk_level="low",
        safe_auto_apply=True,
    ),
    "quarantine_invalid_records": Playbook(
        playbook_id="quarantine_invalid_records",
        summary="Isola registros inválidos em quarentena antes de seguir o pipeline.",
        risk_level="low",
        safe_auto_apply=True,
    ),
    "fallback_to_last_successful_artifacts": Playbook(
        playbook_id="fallback_to_last_successful_artifacts",
        summary="Preserva os últimos artefatos íntegros quando há erro inesperado.",
        risk_level="medium",
        safe_auto_apply=True,
    ),
    "update_pipeline_spec": Playbook(
        playbook_id="update_pipeline_spec",
        summary="Atualiza a spec do pipeline para refletir drift estrutural.",
        risk_level="high",
        safe_auto_apply=False,
    ),
}


def get_playbook(playbook_id: str) -> Playbook:
    return PLAYBOOKS[playbook_id]


def safe_auto_apply_playbooks(compiled_plan: dict[str, Any]) -> set[str]:
    configured = compiled_plan["agent"].get("safe_auto_apply_playbooks", [])
    return set(configured)
