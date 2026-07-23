// Copyright Motia LLC and/or licensed to Motia LLC under one or more
// contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.
// This software is patent protected. We welcome discussions - reach out at team@iii.dev
// See LICENSE and PATENTS files for details.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use iii_helpers::stream::{StreamSetResult, StreamUpdateResult, UpdateOp};
use redis::{AsyncCommands, Client, aio::ConnectionManager};
use serde_json::Value;
use tokio::{sync::Mutex, time::timeout};

use crate::{
    engine::Engine,
    workers::{
        redis::{DEFAULT_REDIS_CONNECTION_TIMEOUT, JSON_UPDATE_SCRIPT},
        state::{
            adapters::StateAdapter,
            registry::{StateAdapterFuture, StateAdapterRegistration},
            structs::{StateListPageItem, StateListPageResult},
        },
    },
};

const STATE_SET_SCRIPT: &str = r#"
    local field = ARGV[1]
    local old_value = redis.call('HGET', KEYS[1], field)
    redis.call('HSET', KEYS[1], field, ARGV[2])
    local active_index = redis.call('GET', KEYS[2])
    if active_index then
        redis.call('ZADD', active_index, 0, field)
    end
    if redis.call('EXISTS', KEYS[3]) == 1 then
        redis.call('ZADD', KEYS[4], 0, field)
        redis.call('ZADD', KEYS[5], 0, field)
    end
    return old_value
"#;

const STATE_DELETE_SCRIPT: &str = r#"
    local field = ARGV[1]
    redis.call('HDEL', KEYS[1], field)
    local active_index = redis.call('GET', KEYS[2])
    if active_index then
        redis.call('ZREM', active_index, field)
    end
    if redis.call('EXISTS', KEYS[3]) == 1 then
        redis.call('ZREM', KEYS[4], field)
        redis.call('ZREM', KEYS[5], field)
    end
    return 1
"#;

const STATE_LIST_PAGE_SCRIPT: &str = r#"
    local active_index = redis.call('GET', KEYS[2])
    if not active_index then
        return {'false', '0', '', 'state pagination index is not ready'}
    end
    local lower = '-'
    if ARGV[1] == '1' then
        lower = '(' .. ARGV[2]
    end
    local limit = tonumber(ARGV[3])
    local keys = redis.call('ZRANGEBYLEX', active_index, lower, '+', 'LIMIT', 0, limit + 1)
    local result = {'true', '0', ''}
    if #keys > limit then
        result[2] = '1'
        result[3] = keys[limit]
    end
    local count = math.min(#keys, limit)
    for i = 1, count do
        local value = redis.call('HGET', KEYS[1], keys[i])
        if not value then
            return {'false', '0', '', 'state pagination index does not match canonical hash'}
        end
        result[#result + 1] = keys[i]
        result[#result + 1] = value
    end
    return result
"#;

const STATE_RECONCILE_SCRIPT: &str = r#"
    if redis.call('GET', KEYS[2]) == KEYS[5] and redis.call('EXISTS', KEYS[3]) == 0 then
        return {'ready'}
    end

    local phase = redis.call('GET', KEYS[3])
    if not phase then
        phase = 'build'
        redis.call('SET', KEYS[3], phase)
        redis.call('SET', KEYS[4], '0')
        redis.call('DEL', KEYS[5], KEYS[6])
    end

    local cursor = redis.call('GET', KEYS[4]) or '0'
    local scan = redis.call('HSCAN', KEYS[1], cursor, 'COUNT', 1000)
    local target = phase == 'build' and KEYS[5] or KEYS[6]
    local entries = scan[2]
    for i = 1, #entries, 2 do
        redis.call('ZADD', target, 0, entries[i])
    end
    redis.call('SET', KEYS[4], scan[1])
    if scan[1] ~= '0' then
        return {'building'}
    end

    if phase == 'build' then
        redis.call('SET', KEYS[3], 'verify')
        redis.call('SET', KEYS[4], '0')
        return {'building'}
    end

    local hash_count = redis.call('HLEN', KEYS[1])
    local build_count = redis.call('ZCARD', KEYS[5])
    local verify_count = redis.call('ZCARD', KEYS[6])
    local build_only = redis.call('ZDIFF', 2, KEYS[5], KEYS[6])
    local verify_only = redis.call('ZDIFF', 2, KEYS[6], KEYS[5])
    if hash_count ~= build_count or build_count ~= verify_count or #build_only ~= 0 or #verify_only ~= 0 then
        redis.call('DEL', KEYS[5], KEYS[6])
        redis.call('SET', KEYS[3], 'build')
        redis.call('SET', KEYS[4], '0')
        return {'building'}
    end

    redis.call('SET', KEYS[2], KEYS[5])
    redis.call('DEL', KEYS[3], KEYS[4], KEYS[6])
    return {'ready'}
"#;

struct StateKeys {
    hash: String,
    active: String,
    phase: String,
    cursor: String,
    target: String,
    verify: String,
}

fn state_keys(scope: &str) -> StateKeys {
    StateKeys {
        hash: format!("state:{scope}"),
        active: format!("iii:state-index-active:{scope}"),
        phase: format!("iii:state-index-phase:{scope}"),
        cursor: format!("iii:state-index-cursor:{scope}"),
        target: format!("iii:state-index:{scope}:v1"),
        verify: format!("iii:state-index-verify:{scope}"),
    }
}

fn state_json_update_script() -> String {
    JSON_UPDATE_SCRIPT.replacen(
        "redis.call('HSET', key, field, new_value_str)",
        r#"redis.call('HSET', key, field, new_value_str)
    local active_index = redis.call('GET', KEYS[2])
    if active_index then
        redis.call('ZADD', active_index, 0, field)
    end
    if redis.call('EXISTS', KEYS[3]) == 1 then
        redis.call('ZADD', KEYS[4], 0, field)
        redis.call('ZADD', KEYS[5], 0, field)
    end"#,
        1,
    )
}

fn decode_list_page_reply(values: Vec<String>) -> anyhow::Result<StateListPageResult> {
    if values.len() < 3 {
        anyhow::bail!("Unexpected return value from state pagination script")
    }
    if values[0] != "true" {
        anyhow::bail!(
            "Failed to list state page: {}",
            values.get(3).map_or("unknown error", String::as_str)
        )
    }
    if (values.len() - 3) % 2 != 0 {
        anyhow::bail!("Unexpected return value from state pagination script")
    }

    let next_cursor = (values[1] == "1").then(|| values[2].clone());
    let mut items = Vec::with_capacity((values.len() - 3) / 2);
    for pair in values[3..].chunks_exact(2) {
        items.push(StateListPageItem {
            key: pair[0].clone(),
            value: serde_json::from_str(&pair[1])
                .map_err(|e| anyhow::anyhow!("Failed to deserialize paged state value: {}", e))?,
        });
    }
    Ok(StateListPageResult { items, next_cursor })
}

pub struct StateRedisAdapter {
    publisher: Arc<Mutex<ConnectionManager>>,
}

impl StateRedisAdapter {
    pub async fn new(redis_url: String) -> anyhow::Result<Self> {
        let client = Client::open(redis_url.as_str())?;
        let manager = timeout(
            DEFAULT_REDIS_CONNECTION_TIMEOUT,
            client.get_connection_manager(),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "Redis connection timed out after {:?}. Please ensure Redis is running at: {}",
                DEFAULT_REDIS_CONNECTION_TIMEOUT,
                redis_url
            )
        })?
        .map_err(|e| anyhow::anyhow!("Failed to connect to Redis at {}: {}", redis_url, e))?;

        let publisher = Arc::new(Mutex::new(manager));
        Ok(Self { publisher })
    }
}

#[async_trait]
impl StateAdapter for StateRedisAdapter {
    async fn set(&self, scope: &str, key: &str, value: Value) -> anyhow::Result<StreamSetResult> {
        let keys = state_keys(scope);
        let mut conn = self.publisher.lock().await;
        let serialized = serde_json::to_string(&value)
            .map_err(|e| anyhow::anyhow!("Failed to serialize value: {}", e))?;

        let script = redis::Script::new(STATE_SET_SCRIPT);

        let result: redis::RedisResult<Option<String>> = script
            .key(&keys.hash)
            .key(&keys.active)
            .key(&keys.phase)
            .key(&keys.target)
            .key(&keys.verify)
            .arg(key)
            .arg(&serialized)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to atomically set value in Redis: {}", e))?;

        match result {
            Ok(s) => {
                let old_value = s.map(|s| serde_json::from_str(&s).unwrap_or(Value::Null));
                let new_value = value.clone();

                Ok(StreamSetResult {
                    old_value,
                    new_value,
                })
            }
            Err(e) => Err(anyhow::anyhow!(
                "Failed to atomically set value in Redis: {}",
                e
            )),
        }
    }

    async fn get(&self, scope: &str, key: &str) -> anyhow::Result<Option<Value>> {
        let scope_key = state_keys(scope).hash;
        let mut conn = self.publisher.lock().await;

        match conn.hget::<_, _, Option<String>>(&scope_key, &key).await {
            Ok(Some(s)) => serde_json::from_str(&s)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize value from Redis: {}", e))
                .map(Some),
            Ok(None) => Ok(None),
            Err(e) => Err(anyhow::anyhow!("Failed to get value from Redis: {}", e)),
        }
    }

    async fn update(
        &self,
        scope: &str,
        key: &str,
        ops: Vec<UpdateOp>,
    ) -> anyhow::Result<StreamUpdateResult> {
        let mut conn = self.publisher.lock().await;
        let keys = state_keys(scope);

        // Serialize operations to JSON
        let ops_json = serde_json::to_string(&ops)
            .map_err(|e| anyhow::anyhow!("Failed to serialize update operations: {}", e))?;

        // Use a single Lua script that atomically gets, applies operations, and sets.
        let update_script = state_json_update_script();
        let script = redis::Script::new(&update_script);

        let result: redis::RedisResult<Vec<String>> = script
            .key(&keys.hash)
            .key(&keys.active)
            .key(&keys.phase)
            .key(&keys.target)
            .key(&keys.verify)
            .arg(key)
            .arg(&ops_json)
            .invoke_async(&mut *conn)
            .await;

        match result {
            Ok(values) if values.len() >= 2 => {
                // Check if the Lua update script reported a failure.
                if values[0] == "false" {
                    return Err(anyhow::anyhow!(
                        "Redis atomic update script failed: {}",
                        values.get(1).map_or("unknown error", String::as_str)
                    ));
                }

                if values.len() == 3 || values.len() == 4 {
                    let old_value = if values[1].is_empty() {
                        None
                    } else {
                        serde_json::from_str(&values[1]).map_err(|e| {
                            anyhow::anyhow!("Failed to deserialize old value: {}", e)
                        })?
                    };

                    let new_value = serde_json::from_str(&values[2])
                        .map_err(|e| anyhow::anyhow!("Failed to deserialize new value: {}", e))?;

                    let errors = if values.len() == 4 && !values[3].is_empty() {
                        serde_json::from_str(&values[3]).map_err(|e| {
                            anyhow::anyhow!("Failed to deserialize update errors: {}", e)
                        })?
                    } else {
                        Vec::new()
                    };

                    Ok(StreamUpdateResult {
                        old_value,
                        new_value,
                        errors,
                    })
                } else {
                    Err(anyhow::anyhow!(
                        "Unexpected return value from update script: expected 3 or 4 values, got {}",
                        values.len()
                    ))
                }
            }
            Err(e) => Err(anyhow::anyhow!("Redis atomic update script failed: {}", e)),
            _ => Err(anyhow::anyhow!(
                "Unexpected return value from update script"
            )),
        }
    }

    async fn delete(&self, scope: &str, key: &str) -> anyhow::Result<()> {
        let keys = state_keys(scope);
        let mut conn = self.publisher.lock().await;

        redis::Script::new(STATE_DELETE_SCRIPT)
            .key(&keys.hash)
            .key(&keys.active)
            .key(&keys.phase)
            .key(&keys.target)
            .key(&keys.verify)
            .arg(key)
            .invoke_async::<i64>(&mut *conn)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to delete value from Redis: {}", e))?;
        Ok(())
    }

    async fn list(&self, scope: &str) -> anyhow::Result<Vec<Value>> {
        let scope_key = format!("state:{}", scope);
        let mut conn = self.publisher.lock().await;

        let values = conn
            .hgetall::<String, HashMap<String, String>>(scope_key)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get group from Redis: {}", e))?;

        let mut result = Vec::new();
        for v in values.into_values() {
            result.push(
                serde_json::from_str(&v)
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize value: {}", e))?,
            );
        }
        Ok(result)
    }

    async fn list_page(
        &self,
        scope: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<StateListPageResult> {
        let keys = state_keys(scope);
        let mut conn = self.publisher.lock().await;

        let reconciliation: Vec<String> = redis::Script::new(STATE_RECONCILE_SCRIPT)
            .key(&keys.hash)
            .key(&keys.active)
            .key(&keys.phase)
            .key(&keys.cursor)
            .key(&keys.target)
            .key(&keys.verify)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to reconcile state pagination index: {}", e))?;
        let values: Vec<String> = redis::Script::new(STATE_LIST_PAGE_SCRIPT)
            .key(&keys.hash)
            .key(&keys.active)
            .arg(if cursor.is_some() { "1" } else { "0" })
            .arg(cursor.unwrap_or_default())
            .arg(limit)
            .invoke_async(&mut *conn)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list state page: {}", e))?;
        let _ = reconciliation;
        decode_list_page_reply(values)
    }

    async fn list_groups(&self) -> anyhow::Result<Vec<String>> {
        let mut conn = self.publisher.lock().await;
        let mut cursor = 0u64;
        let mut groups = Vec::new();

        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("state:*")
                .arg("COUNT")
                .arg(100)
                .query_async(&mut *conn)
                .await?;

            for key in keys {
                if let Some(scope) = key.strip_prefix("state:") {
                    groups.push(scope.to_string());
                }
            }

            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }

        Ok(groups)
    }

    async fn destroy(&self) -> anyhow::Result<()> {
        tracing::debug!("Destroying StateRedisAdapter");
        Ok(())
    }
}

fn make_adapter(_engine: Arc<Engine>, config: Option<Value>) -> StateAdapterFuture {
    Box::pin(async move {
        let redis_url = config
            .as_ref()
            .and_then(|c| c.get("redis_url"))
            .and_then(|v| v.as_str())
            .unwrap_or("redis://localhost:6379")
            .to_string();
        Ok(Arc::new(StateRedisAdapter::new(redis_url).await?) as Arc<dyn StateAdapter>)
    })
}

crate::register_adapter!(<StateAdapterRegistration> name: "redis", make_adapter);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn setup_test_adapter() -> StateRedisAdapter {
        let redis_url = "redis://localhost:6379".to_string();
        StateRedisAdapter::new(redis_url).await.unwrap()
    }

    async fn clear_pagination_scope(adapter: &StateRedisAdapter, scope: &str, extra: &[&str]) {
        let keys = state_keys(scope);
        let mut redis_keys = vec![
            keys.hash,
            keys.active,
            keys.phase,
            keys.cursor,
            keys.target,
            keys.verify,
        ];
        redis_keys.extend(extra.iter().map(|key| (*key).to_string()));
        let mut conn = adapter.publisher.lock().await;
        redis::cmd("DEL")
            .arg(redis_keys)
            .query_async::<i64>(&mut *conn)
            .await
            .unwrap();
    }

    #[test]
    fn pagination_scripts_maintain_fixed_score_indexes_atomically() {
        for script in [STATE_SET_SCRIPT, STATE_DELETE_SCRIPT] {
            assert!(script.contains("ZADD") || script.contains("ZREM"));
            assert!(script.contains("KEYS[2]"));
            assert!(script.contains("KEYS[4]"));
            assert!(script.contains("KEYS[5]"));
        }

        let update_script = state_json_update_script();
        assert!(update_script.contains("redis.call('HSET', key, field, new_value_str)"));
        assert!(update_script.contains("redis.call('ZADD', active_index, 0, field)"));
    }

    #[test]
    fn writes_cannot_bypass_exact_readiness_verification() {
        assert!(!STATE_SET_SCRIPT.contains("'SET', KEYS[2]"));
        assert!(!state_json_update_script().contains("'SET', KEYS[2]"));
        assert!(STATE_RECONCILE_SCRIPT.contains("'SET', KEYS[2], KEYS[5]"));
    }

    #[test]
    fn pagination_script_is_exclusive_bounded_and_fail_closed() {
        assert!(STATE_LIST_PAGE_SCRIPT.contains("'(' .. ARGV[2]"));
        assert!(STATE_LIST_PAGE_SCRIPT.contains("'LIMIT', 0, limit + 1"));
        assert!(STATE_LIST_PAGE_SCRIPT.contains("index is not ready"));
        assert!(!STATE_LIST_PAGE_SCRIPT.contains("HGETALL"));
        assert!(!STATE_LIST_PAGE_SCRIPT.contains("HKEYS"));
    }

    #[test]
    fn reconciliation_is_bounded_and_checks_exact_membership() {
        assert!(STATE_RECONCILE_SCRIPT.contains("'HSCAN'"));
        assert!(STATE_RECONCILE_SCRIPT.contains("'COUNT', 1000"));
        assert!(STATE_RECONCILE_SCRIPT.contains("'ZDIFF'"));
        assert!(STATE_RECONCILE_SCRIPT.contains("'HLEN'"));
        assert!(STATE_RECONCILE_SCRIPT.contains("ZCARD"));
        assert!(STATE_RECONCILE_SCRIPT.contains("'SET', KEYS[2], KEYS[5]"));
        assert!(!STATE_RECONCILE_SCRIPT.contains("HGETALL"));
        assert!(!STATE_RECONCILE_SCRIPT.contains("HKEYS"));
    }

    #[test]
    fn index_keys_do_not_collide_with_state_scope_scan_namespace() {
        let keys = state_keys("scope");
        assert_eq!(keys.hash, "state:scope");
        for key in [
            keys.active,
            keys.phase,
            keys.cursor,
            keys.target,
            keys.verify,
        ] {
            assert!(!key.starts_with("state:"));
        }
    }

    #[test]
    fn list_page_reply_preserves_an_empty_next_cursor() {
        let result = decode_list_page_reply(vec![
            "true".to_string(),
            "1".to_string(),
            String::new(),
            String::new(),
            "1".to_string(),
        ])
        .unwrap();

        assert_eq!(result.next_cursor.as_deref(), Some(""));
        assert_eq!(result.items[0].key, "");
        assert_eq!(result.items[0].value, json!(1));
    }

    #[test]
    fn state_keys_use_a_versioned_target_and_separate_active_pointer() {
        let keys = state_keys("scope");
        assert_eq!(keys.active, "iii:state-index-active:scope");
        assert_eq!(keys.target, "iii:state-index:scope:v1");
        assert_ne!(keys.active, keys.target);
    }

    #[tokio::test]
    #[ignore] // Requires Redis running
    async fn test_list_page_reconciles_legacy_hash_and_round_trips_empty_cursor() {
        let adapter = setup_test_adapter().await;
        let scope = "test_list_page_legacy";
        let keys = state_keys(scope);
        clear_pagination_scope(&adapter, scope, &[]).await;

        {
            let mut conn = adapter.publisher.lock().await;
            redis::cmd("HSET")
                .arg(&keys.hash)
                .arg("")
                .arg("0")
                .arg("a")
                .arg("1")
                .query_async::<i64>(&mut *conn)
                .await
                .unwrap();
        }

        let first_attempt = adapter.list_page(scope, None, 1).await;
        assert!(first_attempt.is_err(), "first legacy build must fail closed");

        let first_page = adapter.list_page(scope, None, 1).await.unwrap();
        assert_eq!(first_page.items[0].key, "");
        assert_eq!(first_page.next_cursor.as_deref(), Some(""));

        let second_page = adapter.list_page(scope, Some(""), 1).await.unwrap();
        assert_eq!(second_page.items[0].key, "a");
        assert_eq!(second_page.next_cursor, None);

        clear_pagination_scope(&adapter, scope, &[]).await;
    }

    #[tokio::test]
    #[ignore] // Requires Redis running
    async fn test_list_page_keeps_old_active_until_verified_target_is_published() {
        let adapter = setup_test_adapter().await;
        let scope = "test_list_page_rebuild";
        let keys = state_keys(scope);
        let old_index = "iii:state-index:test_list_page_rebuild:v0";
        clear_pagination_scope(&adapter, scope, &[old_index]).await;

        {
            let mut conn = adapter.publisher.lock().await;
            redis::cmd("HSET")
                .arg(&keys.hash)
                .arg("a")
                .arg("1")
                .query_async::<i64>(&mut *conn)
                .await
                .unwrap();
            redis::cmd("ZADD")
                .arg(old_index)
                .arg(0)
                .arg("a")
                .query_async::<i64>(&mut *conn)
                .await
                .unwrap();
            redis::cmd("SET")
                .arg(&keys.active)
                .arg(old_index)
                .query_async::<String>(&mut *conn)
                .await
                .unwrap();
        }

        let rebuilding_page = adapter.list_page(scope, None, 10).await.unwrap();
        assert_eq!(rebuilding_page.items[0].key, "a");

        adapter.set(scope, "b", json!(2)).await.unwrap();
        let active_page = adapter.list_page(scope, None, 10).await.unwrap();
        assert_eq!(
            active_page.items.iter().map(|item| item.key.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );

        let active: String = {
            let mut conn = adapter.publisher.lock().await;
            redis::cmd("GET")
                .arg(&keys.active)
                .query_async(&mut *conn)
                .await
                .unwrap()
        };
        assert_eq!(active, keys.target);

        adapter
            .update(scope, "b", vec![UpdateOp::increment("", 1)])
            .await
            .unwrap();
        let updated = adapter.list_page(scope, Some("a"), 10).await.unwrap();
        assert_eq!(updated.items[0].value, json!(3));

        adapter.delete(scope, "b").await.unwrap();
        let deleted = adapter.list_page(scope, None, 10).await.unwrap();
        assert_eq!(deleted.items.len(), 1);
        assert_eq!(deleted.items[0].key, "a");

        clear_pagination_scope(&adapter, scope, &[old_index]).await;
    }

    #[tokio::test]
    #[ignore] // Requires Redis running
    async fn test_list_groups_redis() {
        let adapter = setup_test_adapter().await;

        // Clean up known test keys before running
        let _ = adapter.delete("test_group1", "item1").await;
        let _ = adapter.delete("test_group2", "item1").await;

        // Add test data
        adapter
            .set("test_group1", "item1", json!({"value": 1}))
            .await
            .unwrap();
        adapter
            .set("test_group2", "item1", json!({"value": 2}))
            .await
            .unwrap();

        let mut groups = adapter.list_groups().await.unwrap();
        groups.sort();

        assert!(groups.contains(&"test_group1".to_string()));
        assert!(groups.contains(&"test_group2".to_string()));
    }

    #[tokio::test]
    #[ignore] // Requires Redis running
    async fn test_update_append_redis() {
        let adapter = setup_test_adapter().await;
        let scope = "test_append_scope";
        let key = "append_item";

        let _ = adapter.delete(scope, key).await;
        adapter
            .set(scope, key, json!({"events": [], "transcript": "hello"}))
            .await
            .unwrap();

        let result = adapter
            .update(
                scope,
                key,
                vec![
                    UpdateOp::append("events", json!({"kind": "chunk"})),
                    UpdateOp::append("transcript", json!(" world")),
                ],
            )
            .await
            .unwrap();

        assert_eq!(result.new_value["events"], json!([{"kind": "chunk"}]));
        assert_eq!(result.new_value["transcript"], "hello world");

        let _ = adapter.delete(scope, key).await;
    }

    #[tokio::test]
    #[ignore] // Requires Redis running
    async fn test_update_structured_errors_redis() {
        let adapter = setup_test_adapter().await;
        let scope = "test_error_scope";
        let key = "error_item";

        let _ = adapter.delete(scope, key).await;
        adapter
            .set(
                scope,
                key,
                json!({"bad": "value", "events": {"nested": true}}),
            )
            .await
            .unwrap();

        let result = adapter
            .update(
                scope,
                key,
                vec![
                    UpdateOp::increment("bad", 1),
                    UpdateOp::Set {
                        path: "__proto__".into(),
                        value: Some(json!(true)),
                    },
                    UpdateOp::append("events", json!("chunk")),
                    UpdateOp::Set {
                        path: "ok".into(),
                        value: Some(json!(true)),
                    },
                ],
            )
            .await
            .unwrap();

        assert_eq!(
            result.new_value,
            json!({"bad": "value", "events": {"nested": true}, "ok": true})
        );
        assert_eq!(result.errors.len(), 3);
        assert_eq!(result.errors[0].op_index, 0);
        assert_eq!(result.errors[0].code, "increment.not_number");
        assert_eq!(result.errors[1].op_index, 1);
        assert_eq!(result.errors[1].code, "set.path.proto_polluted");
        assert_eq!(result.errors[2].op_index, 2);
        assert_eq!(result.errors[2].code, "append.type_mismatch");

        let _ = adapter.delete(scope, key).await;
    }

    #[tokio::test]
    #[ignore] // Requires Redis running
    async fn test_update_root_null_and_numeric_null_errors_redis() {
        let adapter = setup_test_adapter().await;
        let scope = "test_null_scope";
        let key = "null_item";

        let _ = adapter.delete(scope, key).await;
        adapter
            .set(scope, key, json!({"": "empty-field", "keep": true}))
            .await
            .unwrap();

        let result = adapter
            .update(
                scope,
                key,
                vec![UpdateOp::Set {
                    path: "".into(),
                    value: None,
                }],
            )
            .await
            .unwrap();

        assert_eq!(result.new_value, json!(null));
        assert!(result.errors.is_empty());

        adapter
            .set(scope, key, json!({"inc": null, "dec": null}))
            .await
            .unwrap();

        let result = adapter
            .update(
                scope,
                key,
                vec![UpdateOp::increment("inc", 1), UpdateOp::decrement("dec", 1)],
            )
            .await
            .unwrap();

        assert_eq!(result.new_value, json!({"inc": null, "dec": null}));
        assert_eq!(result.errors.len(), 2);
        assert_eq!(result.errors[0].op_index, 0);
        assert_eq!(result.errors[0].code, "increment.not_number");
        assert_eq!(result.errors[1].op_index, 1);
        assert_eq!(result.errors[1].code, "decrement.not_number");

        let _ = adapter.delete(scope, key).await;
    }

    #[tokio::test]
    #[ignore] // Requires Redis running
    async fn test_update_empty_path_numeric_and_remove_ops_target_root_redis() {
        let adapter = setup_test_adapter().await;
        let scope = "test_root_ops_scope";
        let key = "root_ops_item";

        let _ = adapter.delete(scope, key).await;
        adapter.set(scope, key, json!(2)).await.unwrap();

        let result = adapter
            .update(scope, key, vec![UpdateOp::increment("", 3)])
            .await
            .unwrap();

        assert_eq!(result.new_value, json!(5));
        assert!(result.errors.is_empty());

        let result = adapter
            .update(scope, key, vec![UpdateOp::decrement("", 2)])
            .await
            .unwrap();

        assert_eq!(result.new_value, json!(3));
        assert!(result.errors.is_empty());

        let result = adapter
            .update(scope, key, vec![UpdateOp::remove("")])
            .await
            .unwrap();

        assert_eq!(result.new_value, json!(null));
        assert!(result.errors.is_empty());

        let _ = adapter.delete(scope, key).await;
    }
}
