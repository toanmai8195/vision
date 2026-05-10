# Phase 5 — k3s + Ollama in Cluster

**Mục tiêu**: Toàn bộ pipeline chạy trên k3s — Ollama deploy như một service trong cluster,
SCION dùng Kubernetes runtime thay vì Docker local.

## Stack

```
k3s cluster (local)
  namespace: vision-agents
  ├── ollama          Deployment + Service (ClusterIP :11434)
  ├── workspace-pvc   PersistentVolumeClaim 20Gi (local-path)
  └── SCION agents    Pod per run (tạo/xóa tự động)

pipeline (host) → kubectl / SCION → k3s
```

## Cài đặt k3s

```bash
curl -sfL https://get.k3s.io | sh
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
kubectl get nodes   # kiểm tra cluster ready
```

## Files thêm

```
agent/k8s/
  ├── namespace.yaml
  ├── ollama-deployment.yaml    # Ollama chạy trong cluster
  ├── ollama-service.yaml       # ClusterIP :11434
  └── workspace-pvc.yaml        # PVC dùng chung cho tất cả agent runs
.scion/
  └── settings.yaml             # cập nhật: runtime=kubernetes
```

## `agent/k8s/namespace.yaml`

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: vision-agents
```

## `agent/k8s/ollama-deployment.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ollama
  namespace: vision-agents
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ollama
  template:
    metadata:
      labels:
        app: ollama
    spec:
      containers:
        - name: ollama
          image: ollama/ollama:latest
          ports:
            - containerPort: 11434
          volumeMounts:
            - name: ollama-data
              mountPath: /root/.ollama
      volumes:
        - name: ollama-data
          emptyDir: {}
```

## `agent/k8s/ollama-service.yaml`

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ollama
  namespace: vision-agents
spec:
  selector:
    app: ollama
  ports:
    - port: 11434
      targetPort: 11434
```

## `agent/k8s/workspace-pvc.yaml`

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: workspace-pvc
  namespace: vision-agents
spec:
  accessModes: [ReadWriteMany]
  storageClassName: local-path
  resources:
    requests:
      storage: 20Gi
```

## `.scion/settings.yaml` cập nhật

```yaml
schema_version: "1"
grove:
  worktree: /workspace
runtimes:
  default: k3s-local
  k3s-local:
    type: kubernetes
    namespace: vision-agents
    kubeconfig: /etc/rancher/k3s/k3s.yaml
```

## `agent/agents/ollama-planner.yaml` cập nhật

```yaml
schema_version: "1"
image: ollama-agent:latest
command: ["python", "planner.py"]
env:
  OLLAMA_BASE_URL: "http://ollama.vision-agents.svc.cluster.local:11434"
  OLLAMA_MODEL: "${OLLAMA_MODEL}"
volumes:
  - name: workspace
    path: /workspace
    claim: workspace-pvc
max_duration: "5m"
```

## Apply

```bash
kubectl apply -f agent/k8s/

# Pull model vào Ollama trong cluster
kubectl exec -n vision-agents deploy/ollama -- ollama pull qwen2.5-coder:7b
```

## Deliverable

- `kubectl get pods -n vision-agents` thấy ollama pod + SCION agent pods khi pipeline chạy
- OLLAMA_BASE_URL trong agent tự trỏ vào service cluster, không phụ thuộc host
- Pipeline chạy end-to-end trên k3s
