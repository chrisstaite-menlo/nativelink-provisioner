use std::collections::HashSet;
use std::time::Duration;

use prometheus_http_query::Client;
use tokio::sync::mpsc::Receiver;

pub(crate) type WorkerName = String;

/// Queries prometheus to find workers that have been idle for a certain period.
async fn get_idle_workers(client: &Client, namespace: &str) -> Option<HashSet<WorkerName>> {
    let query = format!(
        r#"rate(container_cpu_usage_seconds_total{{namespace="{namespace}", container="worker"}} [1m]) < 0.7"#
    );

    let response = match client.query(query).get().await {
        Ok(response) => response,
        Err(err) => {
            tracing::error!(err=?err, "Error getting workers");
            return None;
        }
    };
    let Some(result) = response.data().as_vector() else {
        tracing::info!("No worker response");
        return None;
    };

    let idle_pods = result
        .iter()
        .filter_map(|s| s.metric().get("pod").cloned())
        .collect();

    Some(idle_pods)
}

pub(crate) fn monitor_workers(
    client: Client,
    namespace: String,
    poll_frequency: Duration,
) -> Receiver<HashSet<WorkerName>> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tx.closed() => return,
                maybe_idle_workers = get_idle_workers(&client, &namespace) => {
                    if let Some(idle_workers) = maybe_idle_workers {
                        let _ = tx.send(idle_workers).await;
                    }
                }
            }
            tokio::select! {
                _ = tx.closed() => return,
                _ = tokio::time::sleep(poll_frequency) => (),
            }
        }
    });
    rx
}
