## Prerequisites

- Kubernetes 1.20+

## Installing the Chart

To install the chart with the release name `my-hstream`:

```console
$ helm dependency build deploy/chart/hstream
$ helm install my-hstream deploy/chart/hstream 
```

> **Tip**: List all releases using `helm ls -A`

## Uninstalling the Chart

To uninstall/delete the `my-hstream` deployment:

```console
$ helm delete my-hstream
```
## Contributing

This chart is maintained at [github.com/hstreamdb/hstream](https://github.com/hstreamdb/hstream.git).
