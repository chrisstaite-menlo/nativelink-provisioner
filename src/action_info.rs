// This file monitors the Redis backend of a nativelink scheduler to determine
// the number of jobs that are active for a given set of platform properties.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use bytes::Bytes;
use fred::clients::{Pool, SubscriberClient};
use fred::error::Error;
use fred::prelude::{
    ClientLike, ConnectionConfig, EventInterface, HashesInterface, PerformanceConfig,
    PubsubInterface, ReconnectPolicy,
};
use fred::types::config::{Config, UnresponsiveConfig};
use fred::types::scan::Scanner;
use fred::types::{Builder, Key, Value};
use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc::Receiver;

pub(crate) type PropertyName = String;
pub(crate) type PropertyValue = String;
pub(crate) type PropertySet = BTreeMap<PropertyName, PropertyValue>;
type OperationId = String;
type OperationCount = usize;

/// The name of the field in the Redis hash that stores the data.
const DATA_FIELD_NAME: &str = "data";
/// The key prefix for an AwaitedAction in Redis.
const AWAITED_ACTION_PREFIX: &str = "aa_";

pub(crate) fn monitor_operations(
    redis_addr: &str,
    pub_sub_channel: String,
    group_by: Vec<PropertyName>,
) -> Result<Receiver<(PropertySet, OperationCount)>, Error> {
    let redis_config = Config::from_url_sentinel(redis_addr)?;
    let reconnect_policy = ReconnectPolicy::new_exponential(0, 100, 8000, 2);
    let mut builder = Builder::from_config(redis_config);
    builder
        .set_performance_config(PerformanceConfig {
            default_command_timeout: Duration::from_secs(10),
            broadcast_channel_capacity: 4096,
            ..Default::default()
        })
        .set_connection_config(ConnectionConfig {
            connection_timeout: Duration::from_secs(3),
            internal_command_timeout: Duration::from_secs(10),
            unresponsive: UnresponsiveConfig {
                max_timeout: Some(Duration::from_secs(3)),
                interval: Duration::from_secs(3) / 4,
            },
            ..Default::default()
        })
        .set_policy(reconnect_policy);
    let client_pool = builder.build_pool(1)?;
    let subscriber_client = builder.build_subscriber_client()?;
    client_pool.connect();
    subscriber_client.connect();
    let operation_channel = monitor_changes(subscriber_client, pub_sub_channel);
    Ok(operation_manager(client_pool, operation_channel, group_by))
}

fn monitor_changes(
    subscriber_client: SubscriberClient,
    pub_sub_channel: String,
) -> Receiver<OperationId> {
    let (tx, rx) = tokio::sync::mpsc::channel::<OperationId>(1);
    tokio::spawn(async move {
        let mut rx = subscriber_client.message_rx();
        loop {
            if let Err(err) = subscriber_client.subscribe(&pub_sub_channel).await {
                tracing::error!(err=?err, "Unable to subscribe to {pub_sub_channel}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            let mut reconnect_rx = subscriber_client.reconnect_rx();
            loop {
                let maybe_operation_id = tokio::select! {
                    _ = tx.closed() => {
                        // The parent was dropped, so stop listening.
                        return;
                    }
                    msg = rx.recv() => {
                        // We got a message from the channel.
                        let Ok(msg) = msg else {
                            // Error receiving message, try to reconnect.
                            break;
                        };
                        let Value::String(s) = msg.value else {
                            // We're only interested in string values.
                            continue;
                        };
                        s
                    }
                    _ = reconnect_rx.recv() => {
                        // We reconnected, therefore we need to re-subscribe.
                        break;
                    }
                };
                // If it was an operation ID notification, send the
                // operation ID to the channel.
                if maybe_operation_id.starts_with(AWAITED_ACTION_PREFIX)
                    && tx
                        .send(maybe_operation_id[AWAITED_ACTION_PREFIX.len()..].to_string())
                        .await
                        .is_err()
                {
                    // The parent was dropped, stop listening.
                    return;
                }
            }
            // Give a little while before attempting to reconnect.
            tokio::time::sleep(Duration::from_secs(1)).await;
            tracing::info!("Reconnecting to pub-sub.");
            rx = subscriber_client.message_rx();
            rx.resubscribe();
        }
    });
    rx
}

type QueuedOperations = HashSet<OperationId>;
type RunningOperations = HashSet<OperationId>;
type ActiveOperations = HashMap<PropertySet, (QueuedOperations, RunningOperations)>;


#[derive(Deserialize)]
struct ActionResult {
}

#[derive(Deserialize)]
enum ActionStage {
    Unknown,
    CacheCheck,
    Queued,
    Executing,
    Completed(ActionResult),
}

#[derive(Deserialize)]
struct ActionState {
    stage: ActionStage,
}

#[derive(Deserialize)]
struct ActionInfo {
    platform_properties: HashMap<PropertyName, PropertyValue>,
}

#[derive(Deserialize)]
enum RedisOperationId {
    Uuid(uuid::Uuid),
    String(String),
}

#[derive(Deserialize)]
struct RedisOperation {
    operation_id: RedisOperationId,
    state: ActionState,
    action_info: ActionInfo,
}

enum OperationState {
    PreQueue,
    Queued,
    Running,
    Complete,
}

struct Operation {
    operation_id: OperationId,
    state: OperationState,
    properties: PropertySet,
}

impl RedisOperationId {
    fn into_string(self) -> String {
        match self {
            Self::Uuid(uuid) => uuid.to_string(),
            Self::String(name) => name,
        }
    }
}

async fn list_operations(
    redis_client: &Pool,
    group_by: &HashSet<PropertyName>,
) -> ActiveOperations {
    let mut active_operations = ActiveOperations::new();
    let client = redis_client.next();

    // Scan for all keys starting with "aa_".
    let mut scanner = client.scan(format!("{AWAITED_ACTION_PREFIX}*"), None, None);

    let mut keys: Vec<Key> = Vec::new();
    while let Some(chunk_result) = scanner.next().await {
        match chunk_result {
            Ok(mut chunk) => {
                let Some(results) = chunk.take_results() else {
                    continue;
                };
                keys.extend(results);
            }
            Err(e) => {
                tracing::error!(err = ?e, "Error during scan chunk");
                break;
            }
        }
    }

    for key in keys.into_iter() {
        let Ok(Some(data)) = client
            .hget::<Option<Bytes>, _, _>(key, DATA_FIELD_NAME)
            .await
        else {
            continue;
        };

        let redis_operation: RedisOperation = match serde_json::from_slice(&data) {
            Ok(op) => op,
            Err(e) => {
                tracing::error!(err=?e, operation=?data, "Failed to deserialize operation");
                continue;
            }
        };

        let state = redis_operation.state.stage.into();
        match state {
            OperationState::Queued | OperationState::Running => {
                let properties =
                    filter_properties(group_by, redis_operation.action_info.platform_properties);

                let (queued_operations, running_operations) =
                    active_operations.entry(properties).or_default();

                match state {
                    OperationState::Queued => {
                        queued_operations.insert(redis_operation.operation_id.into_string())
                    }
                    OperationState::Running => {
                        running_operations.insert(redis_operation.operation_id.into_string())
                    }
                    _ => unreachable!(),
                };
            }
            OperationState::PreQueue | OperationState::Complete => {
                // Not an active operation, so we don't add it.
            }
        }
    }

    active_operations
}

fn filter_properties(
    group_by: &HashSet<PropertyName>,
    platform_properties: HashMap<PropertyName, PropertyValue>,
) -> PropertySet {
    platform_properties
        .into_iter()
        .filter(|(property_name, _property_value)| group_by.contains(property_name))
        .collect()
}

impl From<ActionStage> for OperationState {
    fn from(value: ActionStage) -> Self {
        match value {
            ActionStage::Unknown => OperationState::PreQueue,
            ActionStage::CacheCheck => OperationState::PreQueue,
            ActionStage::Queued => OperationState::Queued,
            ActionStage::Executing => OperationState::Running,
            ActionStage::Completed(_) => OperationState::Complete,
        }
    }
}

async fn get_operation(
    redis_client: &Pool,
    group_by: &HashSet<PropertyName>,
    operation_id: &OperationId,
) -> Option<Operation> {
    let key = format!("{AWAITED_ACTION_PREFIX}{operation_id}");
    let client = redis_client.next();
    let data = client
        .hget::<Option<Bytes>, _, _>(&key, Key::from(DATA_FIELD_NAME))
        .await
        .ok()??;
    let redis_operation: RedisOperation = serde_json::from_slice(&data).ok()?;
    let properties = filter_properties(group_by, redis_operation.action_info.platform_properties);
    let state = redis_operation.state.stage.into();
    Some(Operation {
        operation_id: redis_operation.operation_id.into_string(),
        state,
        properties,
    })
}

fn operation_manager(
    redis_client: Pool,
    mut operation_channel: Receiver<OperationId>,
    group_by: Vec<PropertyName>,
) -> Receiver<(PropertySet, OperationCount)> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let group_by = HashSet::from_iter(group_by);
    tokio::spawn(async move {
        let mut active_operations = list_operations(&redis_client, &group_by).await;
        // Initial update for all operations.
        for (properties, (queued, _running)) in &active_operations {
            let queued_entries = queued.len();
            if queued_entries > 0 && tx.send((properties.clone(), queued_entries)).await.is_err() {
                return;
            }
        }
        loop {
            tokio::select! {
                _ = tx.closed() => {
                    return;
                }
                operation_id = operation_channel.recv() => {
                    let Some(operation_id) = operation_id else {
                        return;
                    };
                    let Some(operation) = get_operation(&redis_client, &group_by, &operation_id).await else {
                        continue;
                    };
                    let mut entry = match active_operations.entry(operation.properties) {
                        std::collections::hash_map::Entry::Occupied(entry) => entry,
                        std::collections::hash_map::Entry::Vacant(entry) => entry.insert_entry((HashSet::new(), HashSet::new())),
                    };
                    let (queued_operations, running_operations) = entry.get_mut();
                    let original_size = queued_operations.len();
                    match operation.state {
                        OperationState::Queued => {
                            running_operations.remove(&operation.operation_id);
                            queued_operations.insert(operation.operation_id);
                        }
                        OperationState::Running => {
                            queued_operations.remove(&operation.operation_id);
                            running_operations.insert(operation.operation_id);
                        }
                        OperationState::Complete | OperationState::PreQueue => {
                            running_operations.remove(&operation.operation_id);
                            queued_operations.remove(&operation.operation_id);
                        }
                    }
                    let new_size = queued_operations.len();
                    if new_size != original_size && tx.send((entry.key().clone(), new_size)).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    rx
}
