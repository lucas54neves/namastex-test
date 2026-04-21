from __future__ import annotations

import pandas as pd

from pipeline.publication import sanitize_for_publication


def test_sanitize_for_publication_drops_raw_and_identity_columns_from_silver() -> None:
    df = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "sender_name": "Ana Paula",
                "sender_phone": "+5511999999999",
                "message_body": "oi",
                "conversation_lead_name": "Ana Paula",
                "conversation_agent_name": "Diego Pereira",
                "sender_name_normalized": "ana paula",
                "canonical_lead_name_masked": "XXX XXXXX",
                "lead_contact_ref": "+XXXXXXXXXXXXX",
            }
        ]
    )

    sanitized = sanitize_for_publication(df, "silver")

    assert "sender_name" not in sanitized.columns
    assert "sender_phone" not in sanitized.columns
    assert "message_body" not in sanitized.columns
    assert "conversation_lead_name" not in sanitized.columns
    assert "conversation_agent_name" not in sanitized.columns
    assert "sender_name_normalized" not in sanitized.columns
    assert "canonical_lead_name_masked" in sanitized.columns
    assert "lead_contact_ref" in sanitized.columns


def test_sanitize_for_publication_drops_raw_and_identity_columns_from_silver_messages() -> None:
    df = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "sender_name": "Ana Paula",
                "sender_phone": "+5511999999999",
                "message_body": "oi",
                "conversation_lead_name": "Ana Paula",
                "conversation_agent_name": "Diego Pereira",
                "sender_name_normalized": "ana paula",
                "sender_name_masked": "XXX XXXXX",
                "sender_phone_masked": "+XXXXXXXXXXXXX",
                "message_body_masked": "oi",
            }
        ]
    )

    sanitized = sanitize_for_publication(df, "silver_messages")

    assert "sender_name" not in sanitized.columns
    assert "sender_phone" not in sanitized.columns
    assert "message_body" not in sanitized.columns
    assert "conversation_lead_name" not in sanitized.columns
    assert "conversation_agent_name" not in sanitized.columns
    assert "sender_name_normalized" not in sanitized.columns
    assert "sender_name_masked" in sanitized.columns
    assert "sender_phone_masked" in sanitized.columns
    assert "message_body_masked" in sanitized.columns


def test_sanitize_for_publication_keeps_gold_publish_safe() -> None:
    df = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "lead_name_masked": "XXX XXXXX",
                "sender_name": "Ana Paula",
            }
        ]
    )

    sanitized = sanitize_for_publication(df, "gold")

    assert "sender_name" not in sanitized.columns
    assert "lead_name_masked" in sanitized.columns


def test_sanitize_for_publication_keeps_conversation_enrichment_publish_safe() -> None:
    df = pd.DataFrame(
        [
            {
                "conversation_id": "conv_1",
                "lead_key": "lead_1",
                "explanation_short": "Lead compara concorrente com dado mascarado.",
            }
        ]
    )

    sanitized = sanitize_for_publication(df, "silver_conversations_llm")

    assert sanitized.columns.tolist() == df.columns.tolist()
