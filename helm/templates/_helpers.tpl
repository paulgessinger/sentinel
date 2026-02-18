{{/*
Expand the name of the chart.
*/}}
{{- define "sentinel.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "sentinel.fullname" -}}
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
Create chart label.
*/}}
{{- define "sentinel.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "sentinel.labels" -}}
helm.sh/chart: {{ include "sentinel.chart" . }}
{{ include "sentinel.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "sentinel.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sentinel.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "sentinel.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "sentinel.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding GitHub credentials.
*/}}
{{- define "sentinel.secretName" -}}
{{- if .Values.github.existingSecret }}
{{- .Values.github.existingSecret }}
{{- else }}
{{- include "sentinel.fullname" . }}-github
{{- end }}
{{- end }}

{{/*
Name of the Secret holding Telegram credentials.
*/}}
{{- define "sentinel.telegramSecretName" -}}
{{- if .Values.telegram.existingSecret }}
{{- .Values.telegram.existingSecret }}
{{- else }}
{{- include "sentinel.fullname" . }}-telegram
{{- end }}
{{- end }}

{{/*
Name of the PersistentVolumeClaim.
*/}}
{{- define "sentinel.pvcName" -}}
{{- if .Values.persistence.existingClaim }}
{{- .Values.persistence.existingClaim }}
{{- else }}
{{- include "sentinel.fullname" . }}-data
{{- end }}
{{- end }}

{{/*
Environment variables sourced from the GitHub Secret.
*/}}
{{- define "sentinel.githubSecretEnv" -}}
- name: GITHUB_APP_ID
  valueFrom:
    secretKeyRef:
      name: {{ include "sentinel.secretName" . }}
      key: {{ .Values.github.existingSecretKeys.appId }}
- name: GITHUB_WEBHOOK_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "sentinel.secretName" . }}
      key: {{ .Values.github.existingSecretKeys.webhookSecret }}
- name: GITHUB_PRIVATE_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "sentinel.secretName" . }}
      key: {{ .Values.github.existingSecretKeys.privateKey }}
{{- end }}

{{/*
Environment variables sourced from the ConfigMap.
*/}}
{{- define "sentinel.configMapRef" -}}
envFrom:
  - configMapRef:
      name: {{ include "sentinel.fullname" . }}-config
{{- end }}
