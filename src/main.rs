use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::File;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::{EnvVar, Pod};
use kube::api::{Api, ListParams, PostParams};
use kube::{Client, ResourceExt};
use serde::Deserialize;

mod action_info;
mod worker_monitor;

#[derive(Deserialize)]
struct Config {
    /// The URL for the Redis sentinel which the nativelink scheduler is using.
    redis_address: String,
    /// The pub-sub channel that nativelink notifies with operation changes.
    pub_sub_channel: String,
    /// The Kubernetes namespace to spawn workers into.
    namespace: Option<String>,
    /// The URL of the prometheus frontend to poll for idle workers.
    prometheus_url: String,
    /// How often to check for idle workers.
    #[serde(with = "humantime_serde")]
    poll_frequency: Duration,
    /// Minimum age of a worker before scaling it down.
    #[serde(with = "humantime_serde")]
    minimum_age: Duration,
    /// The maximum number of instances per container image.
    max_pods: usize,
    /// The number of tasks in the queue per worker.
    queue_per_pod: NonZeroUsize,
}

const DOCKER_IMAGE_PREFIX: &str = "docker://";
const CONTAINER_IMAGE_PROPERTY: &str = "container-image";
const CONTAINER_PROPERTY_ENVIRONMENT_NAME: &str = "PP_CONTAINER_IMAGE";

type QueueSize = usize;
type WorkerMap =
    HashMap<action_info::PropertySet, (QueueSize, HashMap<worker_monitor::WorkerName, Instant>)>;

struct ScaleInfo {
    max_pods: usize,
    queue_per_pod: NonZeroUsize,
    minimum_age: Duration,
}

impl ScaleInfo {
    fn required_workers(&self, queue_size: usize) -> usize {
        let required_workers = if queue_size > 0 {
            std::cmp::max(1, queue_size / self.queue_per_pod)
        } else {
            0
        };
        std::cmp::min(required_workers, self.max_pods)
    }

    fn should_scale_up(&self, current_workers: usize, queue_size: usize) -> bool {
        self.required_workers(queue_size) > current_workers
    }

    fn should_scale_down(
        &self,
        start_time: &Instant,
        current_workers: usize,
        queue_size: usize,
    ) -> bool {
        let minimum_uptime = *start_time + self.minimum_age;
        let now = Instant::now();
        let required_workers = self.required_workers(queue_size);
        let decision = minimum_uptime < now && required_workers < current_workers;
        tracing::info!(
            minimum_uptime = ?minimum_uptime,
            now = ?now,
            required_workers = required_workers,
            current_workers = current_workers,
            decision = decision,
            "Should scale down?"
        );
        decision
    }
}

async fn get_worker_pods(
    pods: &Api<Pod>,
    properties: &action_info::PropertySet,
) -> HashMap<worker_monitor::WorkerName, Instant> {
    let Some(image_name) = properties.get(CONTAINER_IMAGE_PROPERTY) else {
        return HashMap::new();
    };
    let Some(image) = image_name.strip_prefix(DOCKER_IMAGE_PREFIX) else {
        return HashMap::new();
    };
    let pod_list = match pods.list(&ListParams::default()).await {
        Ok(list) => list,
        Err(e) => {
            tracing::error!(err = ?e, "Failed to list pods");
            return HashMap::new();
        }
    };
    pod_list
        .into_iter()
        .filter_map(|pod| {
            pod.spec
                .as_ref()?
                .containers
                .iter()
                .any(|container| {
                    container
                        .image
                        .as_ref()
                        .is_some_and(|found_image| found_image == image)
                })
                .then(|| pod.metadata.name.clone())?
                .map(|name| (name, Instant::now()))
        })
        .collect()
}

async fn maybe_scale_up(
    scale_info: &ScaleInfo,
    properties: action_info::PropertySet,
    queue_size: usize,
    pods: &Api<Pod>,
    pod_spec: &Pod,
    workers: &'_ mut WorkerMap,
) {
    let mut workers_entry = match workers.entry(properties.clone()) {
        std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
            occupied_entry.get_mut().0 = queue_size;
            occupied_entry
        }
        std::collections::hash_map::Entry::Vacant(vacant_entry) => {
            let workers = get_worker_pods(pods, vacant_entry.key()).await;
            vacant_entry.insert_entry((queue_size, workers))
        }
    };
    let running_workers = &mut workers_entry.get_mut().1;

    if !scale_info.should_scale_up(running_workers.len(), queue_size) {
        // No need to scale up.
        return;
    }

    // Scale up workers.
    let mut spawn_pod_spec = pod_spec.clone();
    if let Some(image_name) = properties.get(CONTAINER_IMAGE_PROPERTY) {
        if let Some(image) = image_name.strip_prefix(DOCKER_IMAGE_PREFIX) {
            if let Some(spec) = &mut spawn_pod_spec.spec {
                for container in &mut spec.containers {
                    container.image = Some(image.to_string());
                    let mut found = false;
                    for env in container.env.get_or_insert_default() {
                        if env.name == CONTAINER_PROPERTY_ENVIRONMENT_NAME {
                            env.value = Some(image_name.clone());
                            env.value_from = None;
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        container.env.as_mut().unwrap().push(EnvVar {
                            name: CONTAINER_PROPERTY_ENVIRONMENT_NAME.to_string(),
                            value: Some(image_name.clone()),
                            value_from: None,
                        });
                    }
                }
            } else {
                tracing::warn!("No Pod spec found to set environment in");
            }

            // Create a unique name for this pod.
            spawn_pod_spec.metadata.name = Some(format!(
                "{}-{}",
                spawn_pod_spec
                    .metadata
                    .name
                    .as_ref()
                    .map_or("worker", |name| name.as_str()),
                uuid::Uuid::new_v4()
            ));

            tracing::info!(
                queue = queue_size,
                workers = running_workers.len(),
                properties = ?properties,
                "Scaling up a new worker for {image_name}"
            );
            let pp = PostParams::default();
            match pods.create(&pp, &spawn_pod_spec).await {
                Ok(pod) => {
                    let pod_name = pod.name_any();
                    running_workers.insert(pod_name, Instant::now());
                    tracing::info!(workers=?workers, "Scaled up new worker");
                }
                Err(err) => tracing::error!(err=?err, "There was an error creating a container"),
            }
        } else {
            tracing::error!(
                image_name = image_name,
                "Not scaling up because no {DOCKER_IMAGE_PREFIX} prefix"
            );
        }
    } else {
        tracing::error!(properties=?properties, "Not scaling up because no {CONTAINER_IMAGE_PROPERTY} property");
    }
}

async fn maybe_scale_down(
    scale_info: &ScaleInfo,
    idle_workers: HashSet<worker_monitor::WorkerName>,
    workers: &mut WorkerMap,
    pods: &Api<Pod>,
) {
    for idle_worker_name in idle_workers {
        for (_key, (queue_size, worker_names)) in workers.iter_mut() {
            let Some((name, start_time)) = worker_names.remove_entry(&idle_worker_name) else {
                // Worker isn't for this property set, carry on.
                continue;
            };

            // Leave workers if they're still required for the queue.
            if !scale_info.should_scale_down(&start_time, worker_names.len() + 1, *queue_size) {
                worker_names.insert(name, start_time);
                continue;
            }

            // Try to delete the worker.
            tracing::info!(
                queue = *queue_size,
                workers = worker_names.len() + 1,
                "Scaling down worker {idle_worker_name}"
            );
            if let Err(err) = pods.delete(&name, &Default::default()).await {
                tracing::error!(err=?err, "There was an error deleting a container");
                worker_names.insert(name, start_time);
            } else {
                // Workers are unique to each property set, so exit the loop.
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config_path = env::var("CONFIG_PATH").unwrap_or("config.json".to_string());
    let file = File::open(config_path)?;
    let config: Config = serde_json::from_reader(file)?;
    let namespace = match config.namespace {
        Some(namespace) => namespace,
        None => env::var("K8S_NAMESPACE")?,
    };

    let spec_path = env::var("WORKER_SPEC_PATH").unwrap_or("worker-spec.json".to_string());
    let file = File::open(spec_path)?;
    let pod_spec: Pod = serde_json::from_reader(file)?;

    // Get a client to determine the use of the workers.
    let prom_client = prometheus_http_query::Client::try_from(config.prometheus_url.as_str())?;

    // Infer the runtime environment and try to create a Kubernetes Client
    let kube_client = Client::try_default().await?;

    // Read pods in the configured namespace into the typed interface from k8s-openapi
    let pods: Api<Pod> = Api::namespaced(kube_client, &namespace);

    // Monitor for operations in the Redis scheduler.
    let mut operations_change = action_info::monitor_operations(
        &config.redis_address,
        config.pub_sub_channel,
        vec![CONTAINER_IMAGE_PROPERTY.to_string()],
    )?;

    // Find the number of active workers for each property set.
    let mut workers: WorkerMap = HashMap::new();

    // A background task to monitor for idle workers and scale them down.
    let mut idle_worker =
        worker_monitor::monitor_workers(prom_client, namespace, config.poll_frequency);

    // Set up the auto-scaler limits.
    let scale_info = ScaleInfo {
        max_pods: config.max_pods,
        queue_per_pod: config.queue_per_pod,
        minimum_age: config.minimum_age,
    };

    // Wait for there to be a queue or an idle worker.
    tracing::info!("Monitoring queue to scale workers");
    loop {
        tokio::select! {
            idle_workers = idle_worker.recv() => {
                let Some(idle_workers) = idle_workers else {
                    tracing::error!("Idle worker check failed");
                    break;
                };
                maybe_scale_down(&scale_info, idle_workers, &mut workers, &pods).await;
            }
            worker_queue = operations_change.recv() => {
                let Some((properties, queue_size)) = worker_queue else {
                    tracing::error!("Worker queue check failed");
                    break;
                };
                maybe_scale_up(&scale_info, properties, queue_size, &pods, &pod_spec, &mut workers).await;
            }
        }
    }
    tracing::info!("Exiting monitor");

    Ok(())
}
