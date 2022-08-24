{{/*
Expand the name of the chart.
*/}}
{{- define "hstream.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "hstream.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "hstream.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "hstream.labels" -}}
helm.sh/chart: {{ include "hstream.chart" . }}
{{ include "hstream.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "hstream.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hstream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "hstream.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "hstream.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Zookeeper uri
*/}}
{{- define "hstream.zookeeper.uri" -}}
{{- $fullname := (include "hstream.fullname" .) }}
{{- $nodeCount := .Values.zookeeper.replicaCount | int }}
{{- range $index0 := until $nodeCount -}}
{{- $index1 := $index0 | add1 -}}
{{ $fullname }}-zookeeper-{{ $index0 }}.{{ $fullname }}-zookeeper-headless:2181{{ if ne $index1 $nodeCount }},{{ end }}
{{- end }}
{{- end }}

{{/* HStream server seed nodes */}}
{{- define "hstream.seedNodes" -}}
{{- $fullname := (include "hstream.fullname" .) }}
{{- $nodeCount := .Values.replicaCount | int }}
{{- range $index0 := until $nodeCount -}}
{{- $index1 := $index0 | add1 -}}
{{ $fullname }}-{{ $index0 }}.{{ $fullname }}:6571{{ if ne $index1 $nodeCount }},{{ end }}
{{- end }}
{{- end }}
