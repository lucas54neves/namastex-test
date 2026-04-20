from __future__ import annotations

import pandas as pd

from pipeline.transforms import add_conversation_context, deduplicate_events, mask_message_body


def test_mask_message_body_preserves_shape_for_structured_pii() -> None:
    row = pd.Series(
        {
            "message_body": (
                "Ana Paula, meu cpf eh 123.456.789-00, cep 04567-123, "
                "email ana.paula@gmail.com e placa ABC1D23"
            ),
            "sender_name": "Ana Paula",
            "conversation_lead_name": "Ana Paula",
            "conversation_agent_name": "Diego Pereira",
        }
    )

    masked = mask_message_body(row)

    assert "123.456.789-00" not in masked
    assert "04567-123" not in masked
    assert "ana.paula@gmail.com" not in masked
    assert "ABC1D23" not in masked
    assert "XXX.XXX.XXX-XX" in masked
    assert "XXXXX-XXX" in masked
    assert "@xxxxx." in masked
    assert "XXX9X99" in masked


def test_deduplicate_events_keeps_highest_status_priority() -> None:
    df = pd.DataFrame(
        [
            {
                "message_id": "m1",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "direction": "inbound",
                "sender_phone": "+5511999999999",
                "message_type": "text",
                "message_body": "oi",
                "status": "sent",
            },
            {
                "message_id": "m2",
                "conversation_id": "conv_1",
                "timestamp": pd.Timestamp("2026-02-01 10:00:00"),
                "direction": "inbound",
                "sender_phone": "+5511999999999",
                "message_type": "text",
                "message_body": "oi",
                "status": "read",
            },
        ]
    )

    deduped = deduplicate_events(df)

    assert len(deduped) == 1
    assert deduped.iloc[0]["status"] == "read"
    assert deduped.iloc[0]["dropped_duplicate_events"] == 1
    assert bool(deduped.iloc[0]["had_status_duplication"]) is True


def test_conversation_context_uses_first_inbound_and_outbound_names() -> None:
    df = pd.DataFrame(
        [
            {"conversation_id": "conv_1", "direction": "outbound", "sender_name": "Diego"},
            {"conversation_id": "conv_1", "direction": "inbound", "sender_name": "Ana Paula"},
            {"conversation_id": "conv_1", "direction": "inbound", "sender_name": "Ana P."},
        ]
    )

    enriched = add_conversation_context(df)

    assert enriched["conversation_agent_name"].tolist() == ["Diego", "Diego", "Diego"]
    assert enriched["conversation_lead_name"].tolist() == ["Ana Paula", "Ana Paula", "Ana Paula"]
