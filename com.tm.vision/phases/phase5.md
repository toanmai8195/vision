# Phase 5 — Add k3s

**Mục tiêu**: SCION agents chạy trên k3s, workspace qua PersistentVolumeClaim.

## Cài đặt
```bash
curl -sfL https://get.k3s.io | sh
# kubeconfig tự tạo tại: /etc/rancher/k3s/k3s.yaml
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
```

## Files thêm
```
k8s/
  ├── namespace.yaml        # namespace: vision-agents
  └── workspace-pvc.yaml    # PVC 5Gi (local-path, k3s built-in)
.scion/
  └── settings.yaml         # cập nhật: runtime=kubernetes
```

## `k8s/namespace.yaml`
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: vision-agents
```

## `k8s/workspace-pvc.yaml`
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
      storage: 5Gi
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

## `agents/*.yaml` cập nhật
```yaml
volumes:
  - name: workspace
    path: /workspace
```

## Apply
```bash
kubectl apply -f k8s/
```

## Deliverable
- `kubectl get pods -n vision-agents` thấy SCION agent pods khi pipeline chạy
- Pipeline chạy end-to-end trên k3s
