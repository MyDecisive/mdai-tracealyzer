{{- define "debug.values" -}}
{{- fail (printf "GLOBAL=%s" (.Values.global | toJson)) -}}
{{- end -}}


{{/*
Expand the chart name.
*/}}
{{- define "mdai-tracealyzer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "mdai-tracealyzer.fullname" -}}
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
{{- define "mdai-tracealyzer.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "mdai-tracealyzer.labels" -}}
helm.sh/chart: {{ include "mdai-tracealyzer.chart" . }}
{{ include "mdai-tracealyzer.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "mdai-tracealyzer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mdai-tracealyzer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use.
*/}}
{{- define "mdai-tracealyzer.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "mdai-tracealyzer.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}


{{- define "greptimedb.host" -}}
{{- $global := index .Values "global" | default dict -}}
{{- $greptime := index $global "greptime" | default dict -}}
{{- $fullnameOverride := index $greptime "fullnameOverride" | default "" -}}
{{- if $fullnameOverride -}}
{{- printf "%s.%s.svc.cluster.local" $fullnameOverride .Release.Namespace -}}
{{- else -}}
{{- printf "%s-greptimedb.%s.svc.cluster.local" .Release.Name .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{- define "greptimedb.psqlPort" -}}
{{- $cfg := index .Values "greptimedb-standalone" -}}
{{- $port := "4003" -}}
{{- if and $cfg $cfg.postgresServicePort -}}
{{- $port = printf "%v" $cfg.postgresServicePort -}}
{{- end -}}
{{- $port -}}
{{- end -}}

{{- define "greptimedb.mysqlPort" -}}
{{- $cfg := index .Values "global.greptimedb" -}}
{{- $port := "4002" -}}
{{- if and $cfg $cfg.mysqlServicePort -}}
{{- $port = printf "%v" $cfg.mysqlServicePort -}}
{{- end -}}
{{- $port -}}
{{- end -}}

{{- define "greptimedb.grpcPort" -}}
{{- $cfg := index .Values "global.greptimedb" -}}
{{- $port := "4001" -}}
{{- if and $cfg $cfg.grpcServicePort -}}
{{- $port = printf "%v" $cfg.grpcServicePort -}}
{{- end -}}
{{- $port -}}
{{- end -}}

{{- define "greptimedb.database" -}}
{{- $cfg := index .Values "global.greptimedb" -}}
{{- $db := "public" -}}
{{- if and $cfg $cfg.databaseName -}}
{{- $db = $cfg.databaseName -}}
{{- end -}}
{{- $db -}}
{{- end -}}

{{- define "greptimedb.grpcEndpoint" -}}
{{- $endpoint := "mdai-greptimedb.mdai.svc.cluster.local:4001" -}}

{{- if and (hasKey .Values "global") (hasKey .Values.global "greptime") -}}
{{- $endpoint = printf "%s:%s" (include "greptimedb.host" .) (include "greptimedb.grpcPort" .) -}}
{{- else if and (hasKey .Values "config") (hasKey .Values.config "emitter") (hasKey .Values.config.emitter "greptimedbEndpoint") -}}
{{- $endpoint = .Values.config.emitter.greptimedbEndpoint -}}
{{- end -}}

{{- $endpoint -}}
{{- end -}}

{{- define "greptimedb.psqlEndpoint" -}}
{{- $endpoint := "mdai-greptimedb.mdai.svc.cluster.local:4003" -}}

{{- if and (hasKey .Values "global") (hasKey .Values.global "greptime") -}}
{{- $endpoint = printf "%s:%s" (include "greptimedb.host" .) (include "greptimedb.psqlPort" .) -}}
{{- else if and (hasKey .Values "config") (hasKey .Values.config "emitter") (hasKey .Values.config.emitter "greptimedbSqlEndpoint") -}}
{{- $endpoint = .Values.config.emitter.greptimedbSqlEndpoint -}}
{{- end -}}

{{- $endpoint -}}
{{- end -}}
