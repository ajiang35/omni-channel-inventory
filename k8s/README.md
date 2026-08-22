# Kubernetes deployment for Omni Inventory

This directory contains production-style Kubernetes manifests for the application stack.

## Prerequisites

- A Kubernetes cluster with a default StorageClass
- A container registry accessible by the cluster
- `kubectl` configured to the target cluster
- A valid image for the app, for example:
  - `docker build -t your-registry.example.com/omni-inventory:latest .`
  - `docker push your-registry.example.com/omni-inventory:latest`

## Update before deployment

1. Replace the placeholder image in [api.yaml](api.yaml) and [worker.yaml](worker.yaml) with your real registry image.
2. Update [secret.yaml](secret.yaml) with real credentials.
3. Confirm Auth0 values in [configmap.yaml](configmap.yaml).
4. If using a different namespace, update references accordingly.

## Apply manifests

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/api.yaml
kubectl apply -f k8s/worker.yaml
```

## Notes

- The API and worker containers expect environment variables to be provided through Kubernetes Secrets and ConfigMaps.
- The app currently uses `DATABASE_URL` and `KAFKA_BOOTSTRAP_SERVERS` values compatible with Kubernetes service DNS names.
- In production, you would typically also add:
  - Ingress
  - NetworkPolicies
  - PodDisruptionBudgets
  - Prometheus/Grafana instrumentation
  - TLS cert-manager integration
  - External PostgreSQL/Redis/Kafka if managed services are preferred
