# Guide : Auto-Scaling Temporal avec Kubernetes

## Vue d'ensemble : Comment ça marche 🎯

```
Machines Physiques → Kubernetes Cluster → Pods (Workers Temporal)
                          ↓
                    Auto-scaling
                          ↓
                 Ajoute/Retire des Pods
```

## 1. Architecture Kubernetes + Temporal

### Le Cluster Kubernetes
```yaml
# Votre "parc de machines"
Cluster Kubernetes:
  ├── Node 1 (Machine physique/VM - 8 CPU, 32GB RAM)
  ├── Node 2 (Machine physique/VM - 8 CPU, 32GB RAM)  
  ├── Node 3 (Machine GPU - 4 CPU, 16GB RAM, 1 GPU)
  └── Node 4 (Machine GPU - 4 CPU, 16GB RAM, 1 GPU)

# Kubernetes distribue automatiquement vos workers sur ces nodes
```

### Déploiement des Workers Temporal
```yaml
# temporal-worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: research-agent-workers
spec:
  replicas: 3  # Commence avec 3 workers
  selector:
    matchLabels:
      app: temporal-worker
      type: research-agent
  template:
    metadata:
      labels:
        app: temporal-worker
        type: research-agent
    spec:
      containers:
      - name: worker
        image: mycompany/temporal-research-worker:latest
        env:
        - name: TEMPORAL_HOST
          value: "temporal-frontend:7233"
        - name: TASK_QUEUE
          value: "research-agents"
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
          limits:
            cpu: "2"
            memory: "4Gi"
```

## 2. Auto-Scaling : 3 Approches

### Approche 1 : HPA Natif Kubernetes (Le plus simple)
```yaml
# hpa-temporal-workers.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: research-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: research-agent-workers
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70  # Scale si CPU > 70%
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80  # Scale si RAM > 80%
```

### Approche 2 : KEDA avec Métriques Temporal (Recommandé)
```yaml
# keda-scaler.yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: temporal-worker-scaler
spec:
  scaleTargetRef:
    name: research-agent-workers
  minReplicaCount: 2
  maxReplicaCount: 50
  triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus:9090
      metricName: temporal_task_queue_depth
      threshold: '10'  # 10 tasks par worker
      query: |
        temporal_workflow_task_pending{
          namespace="default",
          task_queue="research-agents"
        }
```

### Approche 3 : Script Python Custom
```python
# temporal-autoscaler.py
from kubernetes import client, config
import asyncio
from temporal.client import Client

# Configuration Kubernetes
config.load_incluster_config()  # Si dans le cluster
# ou config.load_kube_config() pour local
k8s_apps = client.AppsV1Api()

async def autoscale_workers():
    # Client Temporal
    temporal_client = await Client.connect("temporal-frontend:7233")
    
    while True:
        # Récupérer métriques Temporal
        queue_metrics = await get_temporal_metrics(temporal_client)
        
        for queue_name, metrics in queue_metrics.items():
            deployment_name = f"{queue_name}-workers"
            
            # Calculer le nombre de workers nécessaires
            desired_workers = calculate_workers(
                pending_tasks=metrics['pending'],
                active_workers=metrics['active'],
                avg_task_duration=metrics['avg_duration']
            )
            
            # Scaler le deployment Kubernetes
            scale_deployment(deployment_name, desired_workers)
        
        await asyncio.sleep(30)

def calculate_workers(pending_tasks, active_workers, avg_task_duration):
    # Logique simple : 1 worker pour 10 tasks
    ideal_workers = max(2, pending_tasks // 10)
    
    # Ne pas scaler trop vite
    if ideal_workers > active_workers:
        return min(active_workers + 5, ideal_workers)  # +5 max
    elif ideal_workers < active_workers:
        return max(2, active_workers - 2)  # -2 max
    
    return active_workers

def scale_deployment(deployment_name, replicas):
    try:
        # Patch le deployment avec le nouveau nombre de replicas
        k8s_apps.patch_namespaced_deployment_scale(
            name=deployment_name,
            namespace="default",
            body={"spec": {"replicas": replicas}}
        )
        print(f"Scaled {deployment_name} to {replicas} replicas")
    except Exception as e:
        print(f"Error scaling: {e}")

if __name__ == "__main__":
    asyncio.run(autoscale_workers())
```

## 3. Configuration du Parc de Machines

### Option A : Cloud Provider (AWS EKS, GCP GKE, Azure AKS)
```bash
# Exemple avec AWS EKS
eksctl create cluster \
  --name temporal-cluster \
  --region us-west-2 \
  --nodegroup-name standard-workers \
  --node-type m5.large \
  --nodes 3 \
  --nodes-min 2 \
  --nodes-max 10 \
  --managed

# Ajouter un node group GPU
eksctl create nodegroup \
  --cluster temporal-cluster \
  --name gpu-workers \
  --node-type p3.2xlarge \
  --nodes 1 \
  --nodes-min 0 \
  --nodes-max 5 \
  --node-labels workload-type=gpu
```

### Option B : On-Premise (Vos propres serveurs)
```bash
# 1. Installer Kubernetes (avec kubeadm)
# Sur le master
kubeadm init --pod-network-cidr=10.244.0.0/16

# 2. Joindre les nodes workers
# Sur chaque machine worker
kubeadm join master-ip:6443 --token xxxxx

# 3. Labeler les nodes selon leur capacité
kubectl label nodes node-gpu-1 node-type=gpu
kubectl label nodes node-cpu-1 node-type=cpu
kubectl label nodes node-mem-1 node-type=high-memory
```

## 4. Déploiement Complet

### Structure des déploiements
```
temporal-namespace/
├── temporal-server/          # Le serveur Temporal
│   ├── frontend
│   ├── history
│   ├── matching
│   └── worker
├── monitoring/              # Prometheus + Grafana
│   ├── prometheus
│   └── grafana
└── workers/                 # Vos workers
    ├── research-agents/     # CPU standard
    ├── ml-agents/          # GPU
    └── data-agents/        # High memory
```

### Exemple : Worker GPU avec contraintes
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ml-agent-workers
spec:
  replicas: 2
  template:
    spec:
      nodeSelector:
        node-type: gpu  # Force sur nodes GPU
      containers:
      - name: ml-worker
        image: mycompany/temporal-ml-worker:latest
        resources:
          limits:
            nvidia.com/gpu: 1  # Réserve 1 GPU
          requests:
            cpu: "2"
            memory: "8Gi"
```

## 5. Monitoring et Dashboards

### Grafana Dashboard pour visualiser
```json
{
  "panels": [
    {
      "title": "Workers par Task Queue",
      "query": "sum by (task_queue) (up{job='temporal-worker'})"
    },
    {
      "title": "Tasks en attente",
      "query": "temporal_workflow_task_pending"
    },
    {
      "title": "Efficacité du scaling",
      "query": "rate(temporal_workflow_task_completed[5m])"
    }
  ]
}
```

## 6. Commandes Utiles

```bash
# Voir l'état des workers
kubectl get pods -l app=temporal-worker

# Voir l'auto-scaling en action
kubectl get hpa -w

# Logs d'un worker
kubectl logs -f deployment/research-agent-workers

# Scaler manuellement
kubectl scale deployment research-agent-workers --replicas=10

# Voir l'utilisation des ressources
kubectl top nodes
kubectl top pods
```

## Résumé : Ce que Kubernetes apporte

1. **Gestion du parc** : Plus besoin de SSH sur chaque machine
2. **Distribution automatique** : K8s place les pods optimalement
3. **Auto-scaling** : Horizontal (plus de pods) et Vertical (plus de ressources)
4. **Self-healing** : Redémarre les workers crashés
5. **Load balancing** : Distribue la charge entre workers
6. **Rolling updates** : Mise à jour sans interruption

C'est comme avoir un "datacenter intelligent" qui gère vos workers Temporal automatiquement !