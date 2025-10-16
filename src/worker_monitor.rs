use std::collections::HashSet;
use std::time::Duration;

use kube::Client;
use kube::api::{Api, ListParams, ObjectMeta};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Receiver;

pub(crate) type WorkerName = String;

#[derive(Deserialize, Serialize, Clone, Debug)]
struct ContainerUsage {
    cpu: String,
    memory: String,
}

// Unchanged: Metrics for a single container
#[derive(Deserialize, Serialize, Clone, Debug)]
struct ContainerMetrics {
    usage: ContainerUsage,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct PodMetrics {
    metadata: ObjectMeta,
    containers: Vec<ContainerMetrics>,
}

impl k8s_openapi::Metadata for PodMetrics {
    type Ty = ObjectMeta;

    fn metadata(&self) -> &<Self as k8s_openapi::Metadata>::Ty {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut <Self as k8s_openapi::Metadata>::Ty {
        &mut self.metadata
    }
}

impl k8s_openapi::Resource for PodMetrics {
    const API_VERSION: &'static str = "metrics.k8s.io/v1beta1";
    const GROUP: &'static str = "metrics.k8s.io";
    const KIND: &'static str = "PodMetrics";
    const VERSION: &'static str = "v1";
    const URL_PATH_SEGMENT: &'static str = "pods";
    type Scope = k8s_openapi::NamespaceResourceScope;
}

/// Queries prometheus to find workers that have been idle for a certain period.
async fn get_idle_workers(client: Client, namespace: &str) -> Option<HashSet<WorkerName>> {
    let pod_metrics_api: Api<PodMetrics> = Api::namespaced(client, namespace);

    let idle_workers = pod_metrics_api
        .list(&ListParams::default())
        .await
        .ok()?
        .into_iter()
        .filter(|pod_metrics| {
            // Filter to component = worker.
            pod_metrics
                .metadata
                .labels
                .as_ref()
                .is_some_and(|metadata| {
                    metadata
                        .get("app.kubernetes.io/component")
                        .map(|component| component.as_str())
                        == Some("worker")
                })
        })
        .filter(|pod_metrics| {
            // Filter CPU usage < 1000000 nanos (1GHz).
            pod_metrics
                .containers
                .iter()
                .any(|metrics| {
                    metrics
                        .usage
                        .cpu
                        .strip_suffix('n')
                        .and_then(|cpu| cpu.parse::<u64>().ok())
                        .is_some_and(|cpu| cpu < 1000000)
                })
        })
        .filter_map(|pod_metrics| pod_metrics.metadata.name)
        .collect::<HashSet<WorkerName>>();

    Some(idle_workers)
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
                maybe_idle_workers = get_idle_workers(client.clone(), &namespace) => {
                    if let Some(idle_workers) = maybe_idle_workers {
                        tracing::info!(idle_workers=?idle_workers, "Idle workers");
                        if tx.send(idle_workers).await.is_err() {
                            return;
                        }
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
