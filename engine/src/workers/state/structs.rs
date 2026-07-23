// Copyright Motia LLC and/or licensed to Motia LLC under one or more
// contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.
// This software is patent protected. We welcome discussions - reach out at team@iii.dev
// See LICENSE and PATENTS files for details.

use iii_helpers::stream::UpdateOp;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StateSetInput {
    /// Namespace that groups related keys (e.g. `users`, `orders`).
    pub scope: String,
    /// Identifier for the value within the scope.
    pub key: String,
    /// Arbitrary JSON value to store. Replaces any existing value at `scope`/`key`.
    #[serde(alias = "data")]
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StateGetInput {
    /// Namespace that groups related keys.
    pub scope: String,
    /// Identifier for the value within the scope.
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StateDeleteInput {
    /// Namespace that groups related keys.
    pub scope: String,
    /// Identifier for the value to delete within the scope.
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StateUpdateInput {
    /// Namespace that groups related keys.
    pub scope: String,
    /// Identifier for the value to update within the scope.
    pub key: String,
    /// Ordered list of update operations applied atomically to the existing value.
    pub ops: Vec<UpdateOp>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StateGetGroupInput {
    /// Namespace whose keys should be listed as a group.
    pub scope: String,
}

fn deserialize_non_null_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StateListPageInput {
    /// Namespace whose entries should be listed.
    pub scope: String,
    /// Opaque key token returned by the previous page. The bound is exclusive.
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub cursor: Option<String>,
    /// Maximum number of entries to return. Defaults to 100 and must be 1..=1000.
    #[serde(
        default,
        deserialize_with = "deserialize_non_null_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct StateListPageItem {
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct StateListPageResult {
    pub items: Vec<StateListPageItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StateListGroupsInput {}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct StateListGroupsResult {
    pub groups: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateEventType {
    #[serde(rename = "state:created")]
    Created,
    #[serde(rename = "state:updated")]
    Updated,
    #[serde(rename = "state:deleted")]
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateEventData {
    #[serde(rename = "type")]
    pub message_type: String,
    pub event_type: StateEventType,
    pub scope: String,
    pub key: String,
    pub old_value: Option<Value>,
    pub new_value: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn state_set_input_data_alias() {
        let json = json!({"scope": "s", "key": "k", "data": "hello"});
        let input: StateSetInput = serde_json::from_value(json).unwrap();
        assert_eq!(input.value, json!("hello"));
    }

    #[test]
    fn state_set_input_value_field() {
        let json = json!({"scope": "s", "key": "k", "value": 42});
        let input: StateSetInput = serde_json::from_value(json).unwrap();
        assert_eq!(input.value, json!(42));
    }

    #[test]
    fn state_event_type_serde() {
        let created = StateEventType::Created;
        let json = serde_json::to_value(&created).unwrap();
        assert_eq!(json, json!("state:created"));

        let back: StateEventType = serde_json::from_value(json!("state:updated")).unwrap();
        assert!(matches!(back, StateEventType::Updated));

        let deleted: StateEventType = serde_json::from_value(json!("state:deleted")).unwrap();
        assert!(matches!(deleted, StateEventType::Deleted));
    }

    #[test]
    fn state_event_data_roundtrip() {
        let json = json!({
            "type": "state_event",
            "event_type": "state:created",
            "scope": "users",
            "key": "user-1",
            "old_value": null,
            "new_value": {"name": "Alice"}
        });
        let data: StateEventData = serde_json::from_value(json).unwrap();
        assert_eq!(data.message_type, "state_event");
        assert!(matches!(data.event_type, StateEventType::Created));
        assert!(data.old_value.is_none());
        let back = serde_json::to_value(&data).unwrap();
        assert_eq!(back["type"], "state_event");
    }

    #[test]
    fn state_event_data_serializes_runtime_message_type() {
        let data = StateEventData {
            message_type: "state".to_string(),
            event_type: StateEventType::Created,
            scope: "users".to_string(),
            key: "user-1".to_string(),
            old_value: None,
            new_value: json!({"name": "Alice"}),
        };

        let json = serde_json::to_value(data).unwrap();
        assert_eq!(json["type"], "state");
        assert_eq!(json["event_type"], "state:created");
    }

    #[test]
    fn state_get_delete_group_roundtrip() {
        let _get: StateGetInput =
            serde_json::from_value(json!({"scope": "s", "key": "k"})).unwrap();
        let _del: StateDeleteInput =
            serde_json::from_value(json!({"scope": "s", "key": "k"})).unwrap();
        let _group: StateGetGroupInput = serde_json::from_value(json!({"scope": "s"})).unwrap();
        let _list: StateListGroupsInput = serde_json::from_value(json!({})).unwrap();
    }

    #[test]
    fn state_list_page_wire_contract() {
        let input: StateListPageInput = serde_json::from_value(json!({
            "scope": "agents",
            "cursor": "agent-100",
            "limit": 25
        }))
        .unwrap();
        assert_eq!(input.scope, "agents");
        assert_eq!(input.cursor.as_deref(), Some("agent-100"));
        assert_eq!(input.limit, Some(25));

        let result = StateListPageResult {
            items: vec![StateListPageItem {
                key: "agent-101".to_string(),
                value: json!({"status": "ready"}),
            }],
            next_cursor: Some("agent-101".to_string()),
        };
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            json!({
                "items": [{"key": "agent-101", "value": {"status": "ready"}}],
                "next_cursor": "agent-101"
            })
        );
    }

    #[test]
    fn state_list_page_omits_absent_optional_fields() {
        let input: StateListPageInput = serde_json::from_value(json!({"scope": "agents"})).unwrap();
        assert_eq!(input.cursor, None);
        assert_eq!(input.limit, None);

        let result = StateListPageResult {
            items: Vec::new(),
            next_cursor: None,
        };
        assert_eq!(serde_json::to_value(result).unwrap(), json!({"items": []}));
    }

    #[test]
    fn state_list_page_rejects_explicit_null_optional_fields() {
        for input in [
            json!({"scope": "agents", "cursor": null}),
            json!({"scope": "agents", "limit": null}),
        ] {
            assert!(serde_json::from_value::<StateListPageInput>(input).is_err());
        }
    }

    #[test]
    fn state_list_page_rejects_malformed_field_types() {
        for input in [
            json!({"scope": "agents", "cursor": 1}),
            json!({"scope": "agents", "cursor": {}}),
            json!({"scope": "agents", "limit": 1.5}),
            json!({"scope": "agents", "limit": "1"}),
        ] {
            assert!(serde_json::from_value::<StateListPageInput>(input).is_err());
        }
    }
}
