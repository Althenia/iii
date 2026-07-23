// Copyright Motia LLC and/or licensed to Motia LLC under one or more
// contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.
// This software is patent protected. We welcome discussions - reach out at team@iii.dev
// See LICENSE and PATENTS files for details.

use std::sync::Arc;

use async_trait::async_trait;
use iii_helpers::stream::{StreamSetResult, StreamUpdateResult, UpdateOp};
use iii_sdk::protocol::TriggerRequest;
use iii_sdk::{IIIClient, InitOptions, register_worker};
use serde_json::Value;

use crate::{
    engine::Engine,
    workers::state::{
        adapters::StateAdapter,
        registry::{StateAdapterFuture, StateAdapterRegistration},
        structs::{
            StateDeleteInput, StateGetGroupInput, StateGetInput, StateListGroupsInput,
            StateListPageInput, StateListPageResult, StateSetInput, StateUpdateInput,
        },
    },
};

pub struct BridgeAdapter {
    bridge: Arc<IIIClient>,
}

impl BridgeAdapter {
    pub async fn new(bridge_url: String) -> anyhow::Result<Self> {
        tracing::info!(bridge_url = %bridge_url, "Connecting to bridge");

        let bridge = Arc::new(register_worker(&bridge_url, InitOptions::default()));

        Ok(Self { bridge })
    }
}

fn list_page_request(scope: &str, cursor: Option<&str>, limit: usize) -> TriggerRequest {
    let data = StateListPageInput {
        scope: scope.to_string(),
        cursor: cursor.map(String::from),
        limit: Some(limit),
    };
    TriggerRequest {
        function_id: "state::list-page".to_string(),
        payload: serde_json::to_value(data).unwrap_or(Value::Null),
        action: None,
        timeout_ms: None,
    }
}

#[async_trait]
impl StateAdapter for BridgeAdapter {
    async fn update(
        &self,
        scope: &str,
        key: &str,
        ops: Vec<UpdateOp>,
    ) -> anyhow::Result<StreamUpdateResult> {
        let data = StateUpdateInput {
            scope: scope.to_string(),
            key: key.to_string(),
            ops,
        };

        let result = self
            .bridge
            .trigger(TriggerRequest {
                function_id: "state::update".to_string(),
                payload: serde_json::to_value(data).unwrap_or(serde_json::Value::Null),
                action: None,
                timeout_ms: None,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to update value via bridge: {}", e))?;

        serde_json::from_value::<StreamUpdateResult>(result)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize update result: {}", e))
    }

    async fn destroy(&self) -> anyhow::Result<()> {
        self.bridge.shutdown_async().await;
        Ok(())
    }

    async fn set(&self, scope: &str, key: &str, value: Value) -> anyhow::Result<StreamSetResult> {
        let data = StateSetInput {
            scope: scope.to_string(),
            key: key.to_string(),
            value,
        };
        let result = self
            .bridge
            .trigger(TriggerRequest {
                function_id: "state::set".to_string(),
                payload: serde_json::to_value(data).unwrap_or(serde_json::Value::Null),
                action: None,
                timeout_ms: None,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to set value via bridge: {}", e))?;

        serde_json::from_value::<StreamSetResult>(result)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize set result: {}", e))
    }

    async fn get(&self, scope: &str, key: &str) -> anyhow::Result<Option<Value>> {
        let data = StateGetInput {
            scope: scope.to_string(),
            key: key.to_string(),
        };
        let result = self
            .bridge
            .trigger(TriggerRequest {
                function_id: "state::get".to_string(),
                payload: serde_json::to_value(data).unwrap_or(serde_json::Value::Null),
                action: None,
                timeout_ms: None,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get value via bridge: {}", e))?;

        serde_json::from_value::<Option<Value>>(result)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize get result: {}", e))
    }

    async fn delete(&self, scope: &str, key: &str) -> anyhow::Result<()> {
        let data = StateDeleteInput {
            scope: scope.to_string(),
            key: key.to_string(),
        };
        self.bridge
            .trigger(TriggerRequest {
                function_id: "state::delete".to_string(),
                payload: serde_json::to_value(data).unwrap_or(serde_json::Value::Null),
                action: None,
                timeout_ms: None,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to delete value via bridge: {}", e))?;
        Ok(())
    }

    async fn list(&self, scope: &str) -> anyhow::Result<Vec<Value>> {
        let data = StateGetGroupInput {
            scope: scope.to_string(),
        };

        let result = self
            .bridge
            .trigger(TriggerRequest {
                function_id: "state::list".to_string(),
                payload: serde_json::to_value(data).unwrap_or(serde_json::Value::Null),
                action: None,
                timeout_ms: None,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list values via bridge: {}", e))?;

        serde_json::from_value::<Vec<Value>>(result)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize list result: {}", e))
    }

    async fn list_page(
        &self,
        scope: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<StateListPageResult> {
        let result = self
            .bridge
            .trigger(list_page_request(scope, cursor, limit))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list page via bridge: {}", e))?;

        serde_json::from_value(result)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize list page result: {}", e))
    }

    async fn list_groups(&self) -> anyhow::Result<Vec<String>> {
        let result = self
            .bridge
            .trigger(TriggerRequest {
                function_id: "state::list_groups".to_string(),
                payload: serde_json::to_value(StateListGroupsInput {})
                    .unwrap_or(serde_json::Value::Null),
                action: None,
                timeout_ms: None,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list groups via bridge: {}", e))?;

        let groups_value = result.get("groups").ok_or_else(|| {
            anyhow::anyhow!("invalid state::list_groups response: missing 'groups' field")
        })?;

        serde_json::from_value::<Vec<String>>(groups_value.clone()).map_err(|e| {
            anyhow::anyhow!(
                "invalid state::list_groups response: invalid 'groups' field: {}",
                e
            )
        })
    }
}

fn make_adapter(_engine: Arc<Engine>, config: Option<Value>) -> StateAdapterFuture {
    Box::pin(async move {
        let bridge_url = config
            .as_ref()
            .and_then(|c| c.get("bridge_url"))
            .and_then(|v| v.as_str())
            .unwrap_or("ws://localhost:49134")
            .to_string();
        Ok(Arc::new(BridgeAdapter::new(bridge_url).await?) as Arc<dyn StateAdapter>)
    })
}

crate::register_adapter!(<StateAdapterRegistration> name: "bridge", make_adapter);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_page_request_forwards_exact_wire_contract() {
        let request = list_page_request("scope", Some("cursor"), 25);
        assert_eq!(request.function_id, "state::list-page");
        assert_eq!(
            request.payload,
            serde_json::json!({"scope": "scope", "cursor": "cursor", "limit": 25})
        );
    }
}
