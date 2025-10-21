// This file monitors the Redis backend of a nativelink scheduler to determine
// the number of jobs that are active for a given set of platform properties.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Duration;

use bytes::Bytes;
use fred::clients::{Pool, SubscriberClient};
use fred::error::{Error, ErrorKind};
use fred::prelude::{
    Client, ClientLike, ConnectionConfig, EventInterface, HashesInterface, PerformanceConfig,
    PubsubInterface, ReconnectPolicy, RediSearchInterface,
};
use fred::types::config::{Config, UnresponsiveConfig};
use fred::types::redisearch::{
    AggregateOperation, FtAggregateOptions, Load, SearchField, WithCursor,
};
use fred::types::{Builder, FromValue, Key, Map, SortOrder, Value};
use serde::Deserialize;
use tokio::sync::mpsc::Receiver;

pub(crate) type PropertyName = String;
pub(crate) type PropertyValue = String;
pub(crate) type PropertySet = BTreeMap<PropertyName, PropertyValue>;
pub(crate) type OperationCount = u64;
type OperationId = String;

/// The name of the field in the Redis hash that stores the data.
const DATA_FIELD_NAME: &str = "data";
/// The name of the field in the Redis hash that stores the data.
const VERSION_FIELD_NAME: &str = "version";
/// The key prefix for an AwaitedAction in Redis.
const AWAITED_ACTION_PREFIX: &str = "aa_";

pub(crate) async fn monitor_operations(
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
    let index_name = find_index(client_pool.next()).await?;
    Ok(operation_manager(
        client_pool,
        operation_channel,
        group_by,
        index_name,
    ))
}

async fn find_index(client: &Client) -> Result<String, Error> {
    let index_list: Vec<String> = client.ft_list().await?;
    index_list
        .into_iter()
        .find(|index| index.starts_with(&format!("{AWAITED_ACTION_PREFIX}_state_sort_key")))
        .ok_or(Error::new(
            fred::error::ErrorKind::InvalidCommand,
            "No index found",
        ))
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
            // Notify that we've connected to get initial operation scan.
            if tx.send(String::new()).await.is_err() {
                return;
            }
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

type CpuCount = u64; // This is scaled by 1000.
type QueuedOperations = HashMap<OperationId, CpuCount>;
type ActiveOperations = HashMap<PropertySet, QueuedOperations>;

#[derive(Deserialize)]
struct ActionResult {}

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
    cpu_count: CpuCount,
}

impl RedisOperationId {
    fn into_string(self) -> String {
        match self {
            Self::Uuid(uuid) => uuid.to_string(),
            Self::String(name) => name,
        }
    }
}

#[derive(Debug, Default)]
struct RedisCursorData {
    total: u64,
    cursor: u64,
    data: VecDeque<Map>,
}

impl FromValue for RedisCursorData {
    fn from_value(value: Value) -> Result<Self, Error> {
        if !value.is_array() {
            return Err(Error::new(ErrorKind::Protocol, "Expected array"));
        }
        let mut output = Self::default();
        let value = value.into_array();
        if value.len() < 2 {
            return Err(Error::new(
                ErrorKind::Protocol,
                "Expected at least 2 elements",
            ));
        }
        let mut value = value.into_iter();
        let data_ary = value.next().unwrap().into_array();
        if data_ary.is_empty() {
            return Err(Error::new(
                ErrorKind::Protocol,
                "Expected at least 1 element in data array",
            ));
        }
        let Some(total) = data_ary[0].as_u64() else {
            return Err(Error::new(
                ErrorKind::Protocol,
                "Expected integer as first element",
            ));
        };
        output.total = total;
        output.data.reserve(data_ary.len() - 1);
        for map_data in data_ary.into_iter().skip(1) {
            output.data.push_back(map_data.into_map()?);
        }
        let Some(cursor) = value.next().unwrap().as_u64() else {
            return Err(Error::new(
                ErrorKind::Protocol,
                "Expected integer as last element",
            ));
        };
        output.cursor = cursor;
        Ok(output)
    }
}

fn get_cpu_count(properties: &HashMap<PropertyName, PropertyValue>) -> CpuCount {
    properties
        .get("cpu_count")
        .and_then(|count| {
            // Round up to the nearest 1000.
            count.parse::<u64>().ok().map(|v| v.div_ceil(1000))
        })
        .unwrap_or(1)
}

async fn list_operations(
    redis_client: &Pool,
    group_by: &HashSet<PropertyName>,
    index_name: &str,
) -> ActiveOperations {
    let mut active_operations = ActiveOperations::new();
    let client = redis_client.next();

    let Ok(mut data): Result<RedisCursorData, _> = client
        .ft_aggregate(
            index_name,
            "@state:{ queued }",
            FtAggregateOptions {
                load: Some(Load::Some(vec![
                    SearchField {
                        identifier: DATA_FIELD_NAME.into(),
                        property: None,
                    },
                    SearchField {
                        identifier: VERSION_FIELD_NAME.into(),
                        property: None,
                    },
                ])),
                cursor: Some(WithCursor {
                    count: Some(256),
                    max_idle: Some(2000),
                }),
                pipeline: vec![AggregateOperation::SortBy {
                    properties: vec![("@sort_key".into(), SortOrder::Asc)],
                    max: None,
                }],
                ..Default::default()
            },
        )
        .await
    else {
        return active_operations;
    };

    loop {
        while let Some(mut map) = data.data.pop_front() {
            if let Some(data) = map
                .remove(&Key::from_static_str(DATA_FIELD_NAME))
                .and_then(|data| data.into_bytes())
            {
                let redis_operation: RedisOperation = match serde_json::from_slice(&data) {
                    Ok(op) => op,
                    Err(e) => {
                        tracing::error!(err=?e, operation=?data, "Failed to deserialize operation");
                        continue;
                    }
                };
                let cpu_count = get_cpu_count(&redis_operation.action_info.platform_properties);
                let properties =
                    filter_properties(group_by, redis_operation.action_info.platform_properties);

                let queued_operations = active_operations.entry(properties).or_default();
                queued_operations.insert(redis_operation.operation_id.into_string(), cpu_count);
            }
        }
        if data.cursor == 0 {
            break;
        }
        match client.ft_cursor_read(index_name, data.cursor, None).await {
            Ok(new_data) => data = new_data,
            Err(_) => break,
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
    let cpu_count = get_cpu_count(&redis_operation.action_info.platform_properties);
    let properties = filter_properties(group_by, redis_operation.action_info.platform_properties);
    let state = redis_operation.state.stage.into();
    Some(Operation {
        operation_id: redis_operation.operation_id.into_string(),
        state,
        properties,
        cpu_count,
    })
}

fn count_queue(operations: &QueuedOperations) -> u64 {
    operations.values().sum()
}

fn operation_manager(
    redis_client: Pool,
    mut operation_channel: Receiver<OperationId>,
    group_by: Vec<PropertyName>,
    index_name: String,
) -> Receiver<(PropertySet, OperationCount)> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let group_by = HashSet::from_iter(group_by);
    let refresh_interval = Duration::from_secs(120);
    tokio::spawn(async move {
        let mut active_operations = HashMap::new();
        loop {
            tokio::select! {
                _ = tx.closed() => {
                    return;
                }
                _ = tokio::time::sleep(refresh_interval) => {
                    // If there were no notifications, then just re-populate.
                    let new_active_operations = list_operations(&redis_client, &group_by, &index_name).await;
                    for (properties, queued) in &new_active_operations {
                        let queue_length = count_queue(queued);
                        tracing::info!(queue=queue_length, properties=?properties, "Refreshed queue");
                        if active_operations.get(properties).is_none_or(|previously_queued: &QueuedOperations| count_queue(previously_queued) != queue_length) && tx.send((properties.clone(), queue_length)).await.is_err() {
                            return;
                        }
                    }
                    // Check for any now zero length queues.
                    for properties in active_operations.keys() {
                        if !new_active_operations.contains_key(properties) {
                            tracing::info!(queue=0, properties=?properties, "Refreshed queue");
                            if tx.send((properties.clone(), 0)).await.is_err() {
                                return;
                            }
                        }
                    }
                    active_operations = new_active_operations;
                }
                operation_id = operation_channel.recv() => {
                    let Some(operation_id) = operation_id else {
                        return;
                    };
                    if operation_id.is_empty() {
                        // Re-populate with all operations.
                        let new_active_operations = list_operations(&redis_client, &group_by, &index_name).await;
                        for (properties, queued) in &new_active_operations {
                            let queue_length = count_queue(queued);
                            tracing::info!(queue=queue_length, properties=?properties, "Refreshed queue");
                            if active_operations.get(properties).is_none_or(|previously_queued: &QueuedOperations| count_queue(previously_queued) != queue_length) && tx.send((properties.clone(), queue_length)).await.is_err() {
                                return;
                            }
                        }
                        // Check for any now zero length queues.
                        for properties in active_operations.keys() {
                            if !new_active_operations.contains_key(properties) {
                                tracing::info!(queue=0, properties=?properties, "Refreshed queue");
                                if tx.send((properties.clone(), 0)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        active_operations = new_active_operations;
                        continue;
                    }
                    let Some(operation) = get_operation(&redis_client, &group_by, &operation_id).await else {
                        continue;
                    };
                    let mut entry = match active_operations.entry(operation.properties) {
                        std::collections::hash_map::Entry::Occupied(entry) => entry,
                        std::collections::hash_map::Entry::Vacant(entry) => entry.insert_entry(QueuedOperations::new()),
                    };
                    let queued_operations = entry.get_mut();
                    let original_size = count_queue(queued_operations);
                    match operation.state {
                        OperationState::Queued => {
                            queued_operations.insert(operation.operation_id, operation.cpu_count);
                        }
                        _ => {
                            queued_operations.remove(&operation.operation_id);
                        }
                    }
                    let new_size = count_queue(queued_operations);
                    if new_size != original_size && tx.send((entry.key().clone(), new_size)).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    rx
}
