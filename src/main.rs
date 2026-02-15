use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::num::NonZeroU64;
use std::path::Path;
use std::pin::Pin;
use std::time::{Duration, Instant};

use futures::{FutureExt, Stream, StreamExt};
use k8s_openapi::api::core::v1::{EnvVar, Pod};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{Api, ListParams, PostParams};
use kube::runtime::watcher::Event;
use kube::{Client, ResourceExt};
use notify::{RecursiveMode, Watcher};
use notify_debouncer_full::{DebounceEventResult, new_debouncer};
use serde::Deserialize;
use tokio::sync::mpsc::Receiver;

mod action_info;

#[derive(Deserialize)]
struct Config {
    /// The URL for the Redis sentinel which the nativelink scheduler is using.
    redis_address: String,
    /// The pub-sub channel that nativelink notifies with operation changes.
    pub_sub_channel: String,
    /// The Kubernetes namespace to spawn workers into.
    namespace: Option<String>,
    /// The maximum number of instances per container image, this does not
    /// include the base worker.
    max_pods: NonZeroU64,
    /// The number of tasks in the queue per allocated CPU.
    queue_per_cpu: NonZeroU64,
    /// The number of CPUs to allocate on the base worker, this is a worker
    /// that remains for two days after a property set is used.
    base_worker_cpu: action_info::OperationCount,
    /// The number of GB of RAM to allocate per CPU allocated to a worker.
    memory_to_cpu: NonZeroU64,
    /// The number of CPUs to allocate to a standard worker.
    worker_cpu: NonZeroU64,
}

const DOCKER_IMAGE_PREFIX: &str = "docker://";
const CONTAINER_IMAGE_PROPERTY: &str = "container-image";
const CONTAINER_PROPERTY_ENVIRONMENT_NAME: &str = "PP_CONTAINER_IMAGE";
const CPU_ENVIRONMENT_NAME: &str = "PP_CPU";
// Keep a base worker laying around for up to two days before getting rid of it.
const BASE_WORKER_AGE: Duration = Duration::from_secs(60 * 60 * 24 * 2);
// Keep a worker for this long to de-bounce queue going up and down.
const SCALE_DEBOUNCE: Duration = Duration::from_secs(90);

type WorkerName = String;

#[derive(Clone, Debug)]
struct WorkerInfo {
    start_time: Instant,
    death_time: Option<Instant>,
}

#[derive(Debug)]
struct PropertyWorkers {
    queue_size: action_info::OperationCount,
    workers: HashMap<WorkerName, WorkerInfo>,
    base_worker: Option<WorkerName>,
    last_queue_update: Instant,
}

impl PropertyWorkers {
    fn new() -> Self {
        Self::new_with_workers(0, HashMap::new())
    }

    fn new_with_workers(
        queue_size: action_info::OperationCount,
        workers: HashMap<WorkerName, WorkerInfo>,
    ) -> Self {
        Self {
            queue_size,
            workers,
            base_worker: None,
            last_queue_update: Instant::now(),
        }
    }

    fn add_worker(&mut self, name: WorkerName) {
        self.add_worker_with_time(name, Instant::now());
    }

    fn add_worker_with_info(&mut self, name: WorkerName, info: WorkerInfo) {
        self.workers.insert(name, info);
    }

    fn add_worker_with_time(&mut self, name: WorkerName, time: Instant) {
        self.add_worker_with_info(
            name,
            WorkerInfo {
                start_time: time,
                death_time: None,
            },
        );
    }

    fn remove_worker(&mut self, name: &WorkerName) -> Option<(WorkerName, Instant)> {
        self.workers
            .remove_entry(name)
            .map(|(name, info)| (name, info.start_time))
    }

    fn update_queue(&mut self, queue_size: action_info::OperationCount) {
        if self.queue_size != 0 || queue_size != 0 {
            self.last_queue_update = Instant::now();
        }
        self.queue_size = queue_size;
    }

    fn base_worker_required(&self) -> bool {
        (Instant::now() - self.last_queue_update) <= BASE_WORKER_AGE
    }

    fn need_base_worker(&self) -> bool {
        self.base_worker.is_none() && self.base_worker_required()
    }

    fn workers_count(&self) -> action_info::OperationCount {
        self.workers.len() as u64
    }

    fn set_base_worker(&mut self, name: WorkerName) -> bool {
        if self.base_worker.is_none() {
            self.base_worker = Some(name);
            true
        } else {
            false
        }
    }

    fn remove_base_worker(&mut self) -> Option<WorkerName> {
        (!self.base_worker_required())
            .then(|| self.base_worker.take())
            .flatten()
    }

    fn remove_if_base_worker(&mut self, worker_name: &WorkerName) -> bool {
        if self
            .base_worker
            .as_ref()
            .is_some_and(|base_worker| base_worker == worker_name)
        {
            self.base_worker.take().is_some()
        } else {
            false
        }
    }

    fn scale_down_to(
        &mut self,
        workers: action_info::OperationCount,
    ) -> Vec<(WorkerName, WorkerInfo)> {
        if self.workers_count() <= workers {
            return Vec::new();
        }
        let to_remove = self.workers_count() - workers;
        let mut all_workers: Vec<_> = self.workers.iter().collect();
        all_workers.sort_by(|a, b| b.1.start_time.cmp(&a.1.start_time));
        let keep_names: Vec<_> = all_workers
            .iter()
            .skip(to_remove as usize)
            .filter_map(|(name, info)| info.death_time.is_some().then_some((*name).clone()))
            .collect();
        all_workers.truncate(to_remove as usize);
        let now: Instant = Instant::now();
        let (to_add_death_time, workers_to_remove): (Vec<_>, Vec<_>) = all_workers
            .into_iter()
            .partition(|(_name, info)| info.death_time.is_none_or(|death_time| death_time >= now));
        let death_names: Vec<_> = to_add_death_time
            .into_iter()
            .filter_map(|(name, info)| info.death_time.is_none().then_some((*name).clone()))
            .collect();
        let removed_workers: Vec<_> = workers_to_remove
            .into_iter()
            .map(|(name, _info)| name.clone())
            .collect::<Vec<_>>()
            .into_iter()
            .filter_map(|name| self.workers.remove_entry(&name))
            .collect();
        // De-bounce scaling.
        for name in death_names {
            if let Some(info) = self.workers.get_mut(&name) {
                tracing::info!("Planning to scale down {name} in {SCALE_DEBOUNCE:?}");
                info.death_time = Some(now + SCALE_DEBOUNCE);
            }
        }
        for name in keep_names {
            if let Some(info) = self.workers.get_mut(&name) {
                tracing::info!("Now going to keep {name}");
                info.death_time = None;
            }
        }
        removed_workers
    }

    fn get_queue_size(&self) -> action_info::OperationCount {
        self.queue_size
    }
}

type WorkerMap = HashMap<action_info::PropertySet, PropertyWorkers>;

struct ScaleInfo {
    max_pods: action_info::OperationCount,
    queue_per_cpu: action_info::OperationCount,
    base_worker_cpu: action_info::OperationCount,
    worker_cpu: action_info::OperationCount,
}

impl ScaleInfo {
    fn required_workers(
        &self,
        queue_size: action_info::OperationCount,
    ) -> action_info::OperationCount {
        let base_queue = self.queue_per_cpu * self.base_worker_cpu;
        let required_workers = if queue_size > base_queue {
            let queue_per_pod = self.queue_per_cpu * self.worker_cpu;
            std::cmp::max(1, queue_size / queue_per_pod)
        } else {
            0
        };
        std::cmp::min(required_workers, self.max_pods)
    }
}

async fn get_worker_pods(
    pods: &Api<Pod>,
    properties: &action_info::PropertySet,
) -> HashMap<WorkerName, WorkerInfo> {
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
                .map(|name| {
                    (
                        name,
                        WorkerInfo {
                            start_time: Instant::now(),
                            death_time: None,
                        },
                    )
                })
        })
        .collect()
}

async fn enumerate_existing_workers(
    pods: &Api<Pod>,
    base_cpu: action_info::OperationCount,
) -> WorkerMap {
    let pod_list = match pods.list(&ListParams::default()).await {
        Ok(list) => list,
        Err(e) => {
            tracing::error!(err = ?e, "Failed to list pods");
            return WorkerMap::new();
        }
    };

    let container_pods = pod_list.into_iter().filter_map(|pod| {
        let name = pod.name_any();
        let container = pod.spec?.containers.into_iter().next()?;
        let image_name = container
            .env?
            .into_iter()
            .find(|var| var.name == CONTAINER_PROPERTY_ENVIRONMENT_NAME)?
            .value?;
        let is_base = container
            .resources
            .as_ref()
            .and_then(|resources| resources.requests.as_ref())
            .and_then(|requests| requests.get("cpu"))
            .and_then(|cpu| cpu.0.parse::<action_info::OperationCount>().ok())
            .is_some_and(|request_cpu| request_cpu == base_cpu);
        Some((image_name, name, is_base))
    });

    let mut worker_map = WorkerMap::new();
    for (container, name, is_base) in container_pods {
        let properties =
            action_info::PropertySet::from([(CONTAINER_IMAGE_PROPERTY.to_string(), container)]);
        match worker_map.entry(properties) {
            std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                if !is_base || !occupied_entry.get_mut().set_base_worker(name.clone()) {
                    occupied_entry.get_mut().add_worker(name);
                }
            }
            std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                let occupied_entry = vacant_entry.insert(PropertyWorkers::new());
                if !is_base || !occupied_entry.set_base_worker(name.clone()) {
                    occupied_entry.add_worker(name);
                }
            }
        }
    }
    worker_map
}

struct WorkerManager {
    namespace: String,
    scale_info: ScaleInfo,
    pods: Api<Pod>,
    pod_spec: Pod,
    workers: WorkerMap,
    operations_change: Receiver<(action_info::PropertySet, action_info::OperationCount)>,
    watcher: Pin<Box<dyn Stream<Item = kube::runtime::watcher::Result<Event<Pod>>>>>,
    memory_to_cpu: action_info::OperationCount,
    worker_cpu: action_info::OperationCount,
    base_worker_cpu: action_info::OperationCount,
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
            max_pods: config.max_pods.get(),
            queue_per_cpu: config.queue_per_cpu.get(),
            base_worker_cpu: config.base_worker_cpu,
            worker_cpu: config.worker_cpu.get(),
        };
        // Read pods in the configured namespace into the typed interface from k8s-openapi
        let pods: Api<Pod> = Api::namespaced(kube_client.clone(), &namespace);

        // Monitor for operations in the Redis scheduler.
        let operations_change = action_info::monitor_operations(
            &config.redis_address,
            config.pub_sub_channel,
            vec![CONTAINER_IMAGE_PROPERTY.to_string()],
        )
        .await?;

        let watcher = Box::pin(kube::runtime::watcher(pods.clone(), Default::default()));

        let workers = enumerate_existing_workers(&pods, config.base_worker_cpu).await;

        Ok(Self {
            namespace,
            scale_info,
            pods,
            pod_spec,
            workers,
            operations_change,
            watcher,
            memory_to_cpu: config.memory_to_cpu.get(),
            worker_cpu: config.worker_cpu.get(),
            base_worker_cpu: config.base_worker_cpu,
        })
    }

    fn get_pod_spec(mut spawn_pod_spec: Pod, properties: &action_info::PropertySet) -> Option<Pod> {
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

                return Some(spawn_pod_spec);
            } else {
                tracing::error!(
                    image_name = image_name,
                    "Not scaling up because no {DOCKER_IMAGE_PREFIX} prefix"
                );
            }
        } else {
            tracing::error!(properties=?properties, "Not scaling up because no {CONTAINER_IMAGE_PROPERTY} property");
        }
        None
    }

    fn configure_pod(
        pod: &Pod,
        cpu: action_info::OperationCount,
        memory: action_info::OperationCount,
    ) -> Pod {
        let mut this_pod = pod.clone();
        // Create a unique name for this pod.
        this_pod.metadata.name = Some(format!(
            "{}-{}",
            pod.metadata
                .name
                .as_ref()
                .map_or("worker", |name| name.as_str()),
            uuid::Uuid::new_v4()
        ));

        if let Some(spec) = &mut this_pod.spec {
            for container in &mut spec.containers {
                let mut found = false;
                let cpu_env = format!("{}", cpu * 1000);
                for env in container.env.get_or_insert_default() {
                    if env.name == CPU_ENVIRONMENT_NAME {
                        env.value = Some(cpu_env.clone());
                        env.value_from = None;
                        found = true;
                        break;
                    }
                }
                if !found {
                    container.env.as_mut().unwrap().push(EnvVar {
                        name: CPU_ENVIRONMENT_NAME.to_string(),
                        value: Some(cpu_env),
                        value_from: None,
                    });
                }
                let resources = container.resources.get_or_insert_default();
                let requests = resources.requests.get_or_insert_default();
                requests.insert("cpu".into(), Quantity(format!("{cpu}")));
                requests.insert("memory".into(), Quantity(format!("{memory}Gi")));
                let limits = resources.limits.get_or_insert_default();
                limits.insert("cpu".into(), Quantity(format!("{cpu}")));
                limits.insert("memory".into(), Quantity(format!("{memory}Gi")));
            }
        }

        this_pod
    }

    async fn maybe_scale_up(
        &mut self,
        properties: action_info::PropertySet,
        queue_size: action_info::OperationCount,
    ) {
        let mut workers_entry = match self.workers.entry(properties.clone()) {
            std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                occupied_entry.get_mut().update_queue(queue_size);
                occupied_entry
            }
            std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                let workers = get_worker_pods(&self.pods, vacant_entry.key()).await;
                vacant_entry.insert_entry(PropertyWorkers::new_with_workers(queue_size, workers))
            }
        };
        let property_workers = workers_entry.get_mut();
        let need_base_worker = property_workers.need_base_worker();

        let required_workers = self.scale_info.required_workers(queue_size);
        if property_workers.workers_count() >= required_workers && !need_base_worker {
            // No need to scale up.
            return;
        }

        if let Some(spawn_pod_spec) = Self::get_pod_spec(self.pod_spec.clone(), &properties) {
            if need_base_worker {
                let this_pod = Self::configure_pod(
                    &spawn_pod_spec,
                    self.base_worker_cpu,
                    self.base_worker_cpu * self.memory_to_cpu,
                );

                match self.pods.create(&PostParams::default(), &this_pod).await {
                    Ok(pod) => {
                        if !property_workers.set_base_worker(pod.name_any()) {
                            // This should never happen, but clean up in case.
                            let name = pod.name_any();
                            tracing::info!(
                                properties = ?properties,
                                "Scaled a new base worker {name}"
                            );
                            if let Err(err) = self.pods.delete(&name, &Default::default()).await {
                                tracing::error!(err=?err, "There was an error deleting a container {name}");
                            }
                        }
                    }
                    Err(err) => {
                        tracing::error!(err=?err, "There was an error creating a container");
                    }
                }
            }

            let worker_count = property_workers.workers_count();
            for _ in worker_count..required_workers {
                let this_pod = Self::configure_pod(
                    &spawn_pod_spec,
                    self.worker_cpu,
                    self.worker_cpu * self.memory_to_cpu,
                );
                match self.pods.create(&PostParams::default(), &this_pod).await {
                    Ok(pod) => {
                        property_workers.add_worker(pod.name_any());
                        tracing::info!(
                            queue = queue_size,
                            workers = property_workers.workers_count(),
                            properties = ?properties,
                            "Scaling up a new worker {}", pod.name_any()
                        );
                    }
                    Err(err) => {
                        tracing::error!(err=?err, "There was an error creating a container");
                        break;
                    }
                }
            }
        }
    }

    fn is_404(error: &kube::Error) -> bool {
        if let kube::Error::Api(error_response) = &error
            && error_response.code == 404
        {
            return true;
        }
        false
    }

    async fn maybe_scale_down(&mut self) -> bool {
        let mut schedule_scale_down = false;
        for (_key, property_workers) in self.workers.iter_mut() {
            if let Some(base_worker) = property_workers.remove_base_worker()
                && let Err(err) = self.pods.delete(&base_worker, &Default::default()).await
                && !Self::is_404(&err)
            {
                property_workers.set_base_worker(base_worker);
                tracing::error!(err=?err, "There was an error deleting a container");
            }

            let required_workers = self
                .scale_info
                .required_workers(property_workers.get_queue_size());
            for (worker_name, worker_info) in property_workers.scale_down_to(required_workers) {
                // Try to delete the worker.
                tracing::info!(
                    queue = property_workers.get_queue_size(),
                    workers = property_workers.workers_count() + 1,
                    "Scaling down worker {worker_name}"
                );
                if let Err(err) = self.pods.delete(&worker_name, &Default::default()).await
                    && !Self::is_404(&err)
                {
                    tracing::error!(err=?err, "There was an error deleting a container");
                    property_workers.add_worker_with_info(worker_name, worker_info);
                }
            }

            if property_workers.workers_count() > required_workers {
                schedule_scale_down = true;
            }
        }
        schedule_scale_down
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
        for (properties, property_workers) in self.workers.iter_mut() {
            if property_workers.remove_worker(&pod_name).is_some()
                || property_workers.remove_if_base_worker(&pod_name)
            {
                tracing::info!(phase = status.phase, "Worker stopped running: {}", pod_name);
                let _ = self.pods.delete(&pod_name, &Default::default()).await;
                deleted = Some((properties.clone(), property_workers.get_queue_size()));
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
    let mut scale_down_retry = None;
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
            worker_queue = manager.operations_change.recv() => {
                let Some((properties, queue_size)) = worker_queue else {
                    tracing::error!("Worker queue check failed");
                    break;
                };
                manager.maybe_scale_up(properties, queue_size).await;
                scale_down_retry = manager.maybe_scale_down().await.then_some(Duration::from_secs(10));
            }
            () = scale_down_retry.map_or(core::future::pending().left_future(), |duration| tokio::time::sleep(duration).right_future()) => {
                scale_down_retry = manager.maybe_scale_down().await.then_some(Duration::from_secs(10));
            }
        }
    }
    tracing::info!("Exiting monitor");

    Ok(())
}
