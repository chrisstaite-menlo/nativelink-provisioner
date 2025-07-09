use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::File;
use std::num::NonZeroUsize;
use std::path::Path;
use std::pin::Pin;
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};
use k8s_openapi::api::core::v1::{EnvVar, Pod};
use kube::api::{Api, ListParams, PostParams};
use kube::runtime::watcher::Event;
use kube::{Client, ResourceExt};
use notify::{RecursiveMode, Watcher};
use notify_debouncer_full::{DebounceEventResult, new_debouncer};
use serde::Deserialize;
use tokio::sync::mpsc::Receiver;

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

    fn should_scale_down(
        &self,
        start_time: &Instant,
        current_workers: usize,
        queue_size: usize,
    ) -> bool {
        let minimum_uptime = *start_time + self.minimum_age;
        let now = Instant::now();
        let required_workers = self.required_workers(queue_size);
        minimum_uptime < now && required_workers < current_workers
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

struct WorkerManager {
    namespace: String,
    scale_info: ScaleInfo,
    pods: Api<Pod>,
    pod_spec: Pod,
    workers: WorkerMap,
    idle_worker: Receiver<HashSet<String>>,
    operations_change: Receiver<(action_info::PropertySet, action_info::OperationCount)>,
    watcher: Pin<Box<dyn Stream<Item = kube::runtime::watcher::Result<Event<Pod>>>>>,
}

impl WorkerManager {
    async fn new(
        kube_client: Client,
        config: Config,
        pod_spec: Pod,
        namespace: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Set up the auto-scaler limits.
        let scale_info = ScaleInfo {
            max_pods: config.max_pods,
            queue_per_pod: config.queue_per_pod,
            minimum_age: config.minimum_age,
        };
        // Read pods in the configured namespace into the typed interface from k8s-openapi
        let pods: Api<Pod> = Api::namespaced(kube_client, &namespace);

        // A background task to monitor for idle workers and scale them down.
        let prom_client = prometheus_http_query::Client::try_from(config.prometheus_url.as_str())?;
        let idle_worker =
            worker_monitor::monitor_workers(prom_client, namespace.clone(), config.poll_frequency);

        // Monitor for operations in the Redis scheduler.
        let operations_change = action_info::monitor_operations(
            &config.redis_address,
            config.pub_sub_channel,
            vec![CONTAINER_IMAGE_PROPERTY.to_string()],
        )
        .await?;

        let watcher = Box::pin(kube::runtime::watcher(pods.clone(), Default::default()));

        Ok(Self {
            namespace,
            scale_info,
            pods,
            pod_spec,
            workers: WorkerMap::new(),
            idle_worker,
            operations_change,
            watcher,
        })
    }

    async fn maybe_scale_up(&mut self, properties: action_info::PropertySet, queue_size: usize) {
        let mut workers_entry = match self.workers.entry(properties.clone()) {
            std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                occupied_entry.get_mut().0 = queue_size;
                occupied_entry
            }
            std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                let workers = get_worker_pods(&self.pods, vacant_entry.key()).await;
                vacant_entry.insert_entry((queue_size, workers))
            }
        };
        let running_workers = &mut workers_entry.get_mut().1;

        let current_workers = running_workers.len();
        let required_workers = self.scale_info.required_workers(queue_size);
        if current_workers >= required_workers {
            // No need to scale up.
            return;
        }

        // Scale up workers.
        let mut spawn_pod_spec = self.pod_spec.clone();
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

                for _ in current_workers..required_workers {
                    // Create a unique name for this pod.
                    let mut this_pod = spawn_pod_spec.clone();
                    this_pod.metadata.name = Some(format!(
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
                    match self.pods.create(&pp, &this_pod).await {
                        Ok(pod) => {
                            let pod_name = pod.name_any();
                            running_workers.insert(pod_name, Instant::now());
                        }
                        Err(err) => {
                            tracing::error!(err=?err, "There was an error creating a container");
                            break;
                        }
                    }
                }

                tracing::info!(workers=?self.workers, "Scaled up new workers");
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

    async fn maybe_scale_down(&mut self, idle_workers: HashSet<worker_monitor::WorkerName>) {
        for idle_worker_name in idle_workers {
            for (_key, (queue_size, worker_names)) in self.workers.iter_mut() {
                let Some((name, start_time)) = worker_names.remove_entry(&idle_worker_name) else {
                    // Worker isn't for this property set, carry on.
                    continue;
                };

                // Leave workers if they're still required for the queue.
                if !self.scale_info.should_scale_down(
                    &start_time,
                    worker_names.len() + 1,
                    *queue_size,
                ) {
                    tracing::info!(
                        queue_size = *queue_size,
                        workers = worker_names.len() + 1,
                        "Not scaling down worker {name} yet."
                    );
                    worker_names.insert(name, start_time);
                    continue;
                }

                // Try to delete the worker.
                tracing::info!(
                    queue = *queue_size,
                    workers = worker_names.len() + 1,
                    "Scaling down worker {idle_worker_name}"
                );
                if let Err(err) = self.pods.delete(&name, &Default::default()).await {
                    tracing::error!(err=?err, "There was an error deleting a container");
                    worker_names.insert(name, start_time);
                } else {
                    // Workers are unique to each property set, so exit the loop.
                    break;
                }
            }
        }
    }

    async fn handle_pod_notification(&mut self, pod: Pod) {
        let Some(status) = &pod.status else {
            return;
        };
        if status
            .phase
            .as_ref()
            .is_some_and(|phase| phase == "Running" || phase == "Pending")
        {
            return;
        }
        let pod_name = pod.name_any();
        let mut deleted = None;
        for (properties, (queue_size, worker_names)) in self.workers.iter_mut() {
            if worker_names.remove(&pod_name).is_some() {
                tracing::info!(phase = status.phase, "Worker stopped running: {}", pod_name);
                let _ = self.pods.delete(&pod_name, &Default::default()).await;
                deleted = Some((properties.clone(), *queue_size));
                break;
            }
        }
        if let Some((properties, queue_size)) = deleted {
            self.maybe_scale_up(properties, queue_size).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config_path = env::var("CONFIG_PATH").unwrap_or("config.json".to_string());
    let config: Config = serde_json::from_reader(File::open(&config_path)?)?;
    let namespace = match &config.namespace {
        Some(namespace) => namespace.clone(),
        None => env::var("K8S_NAMESPACE")?,
    };

    let spec_path = env::var("WORKER_SPEC_PATH").unwrap_or("worker-spec.json".to_string());
    let pod_spec: Pod = serde_json::from_reader(File::open(&spec_path)?)?;

    // Configuration re-load.
    let (tx, mut config_changed) = tokio::sync::mpsc::channel(1);
    let rt = tokio::runtime::Handle::current();
    let mut debouncer = new_debouncer(
        Duration::from_secs(2),
        None,
        move |result: DebounceEventResult| {
            let tx = tx.clone();
            rt.spawn(async move {
                let _ = tx.send(result).await;
            });
        },
    )?;
    debouncer
        .watcher()
        .watch(Path::new(&spec_path), RecursiveMode::NonRecursive)?;
    debouncer
        .watcher()
        .watch(Path::new(&config_path), RecursiveMode::NonRecursive)?;

    // Infer the runtime environment and try to create a Kubernetes Client
    let kube_client = Client::try_default().await?;

    // Wait for there to be a queue or an idle worker.
    tracing::info!("Monitoring queue to scale workers");
    let mut manager = WorkerManager::new(kube_client.clone(), config, pod_spec, namespace).await?;
    loop {
        tokio::select! {
            maybe_changed_config = config_changed.recv() => {
                let Some(Ok(_changed_config)) = maybe_changed_config else {
                    tracing::error!("Config watcher exited");
                    break;
                };
                // Reload the configuration.
                if let Ok(file) = File::open(&config_path) {
                    match serde_json::from_reader::<_, Config>(file) {
                        Ok(new_config) => {
                            let namespace = new_config.namespace.as_ref().unwrap_or(&manager.namespace).clone();
                            match WorkerManager::new(kube_client.clone(), new_config, manager.pod_spec.clone(), namespace).await {
                                 Ok(new_manager) => manager = new_manager,
                                 Err(err) => tracing::error!(err=?err, "New configuration error"),
                            }
                        }
                        Err(err) => tracing::error!(err=?err, "Error re-loading configuration file"),
                    }
                }
                if let Ok(file) = File::open(&spec_path) {
                    match serde_json::from_reader(file) {
                        Ok(new_pod_spec) => manager.pod_spec = new_pod_spec,
                        Err(err) => tracing::error!(err=?err, "Error re-loading pod spec file"),
                    }
                }
                tracing::info!("Configuration reload complete");
            }
            maybe_pod_change = manager.watcher.next() => {
                let Some(Ok(change)) = maybe_pod_change else {
                    tracing::error!("Error watching for pod changes");
                    break;
                };
                match change {
                    kube::runtime::watcher::Event::InitApply(pod) | kube::runtime::watcher::Event::Apply(pod) | kube::runtime::watcher::Event::Delete(pod) => {
                        manager.handle_pod_notification(pod).await;
                    }
                    kube::runtime::watcher::Event::Init | kube::runtime::watcher::Event::InitDone => (),
                }
            }
            idle_workers = manager.idle_worker.recv() => {
                let Some(idle_workers) = idle_workers else {
                    tracing::error!("Idle worker check failed");
                    break;
                };
                manager.maybe_scale_down(idle_workers).await;
            }
            worker_queue = manager.operations_change.recv() => {
                let Some((properties, queue_size)) = worker_queue else {
                    tracing::error!("Worker queue check failed");
                    break;
                };
                manager.maybe_scale_up(properties, queue_size).await;
            }
        }
    }
    tracing::info!("Exiting monitor");

    Ok(())
}
