#!/usr/bin/env bash
# Crea los 14 Issues del proyecto con GitHub CLI.
# Uso:  ./scripts/crear_issues.sh <owner/repo>
# Requisitos: gh instalado y autenticado (gh auth login)
set -euo pipefail

REPO="${1:?Uso: $0 <owner/repo>}"

crear() {
  # $1 titulo, $2 etiquetas, $3 cuerpo
  gh issue create --repo "$REPO" --title "$1" --label "$2" --body "$3"
}

# Etiquetas (ignora error si ya existen)
for l in semana-1 semana-2 semana-3 semana-4 arquitectura contrato procesamiento kafka operacion docs producer spark data-lake tests-ci airflow calidad release; do
  gh label create "$l" --repo "$REPO" --force >/dev/null 2>&1 || true
done

crear "Definir arquitectura específica de la variante" "semana-1,arquitectura" "Diagrama y descripción de componentes para Riesgo transaccional. Entregable: docs/arquitectura.png"
crear "Definir contrato JSON del evento y sample" "semana-1,contrato" "docs/contrato-evento.md y samples/payment_event.json"
crear "Definir 3 KPIs y 2 reglas de alerta con umbrales" "semana-1,procesamiento" "docs/reglas-kpis-alertas.md"
crear "Definir topic, key y particiones de Kafka (ADR-001)" "semana-1,kafka" "docs/decisions/ADR-001-key-particiones-kafka.md"
crear "Crear estructura del repo y docker-compose preliminar" "semana-1,operacion" "Carpetas, .gitignore, requirements.txt y docker-compose.yml"
crear "README inicial y checkpoint de la semana 1" "semana-1,docs" "README.md y docs/checkpoints/semana1.md (máx. 1 página)"
crear "Implementar producer de eventos de pago" "semana-2,producer" "Eventos normales y anómalos, identificables por equipo"
crear "Configurar Kafka con particiones y key" "semana-2,kafka" "Topic con 3 particiones y key order_id"
crear "Spark Structured Streaming: 3 transformaciones y ventana" "semana-2,spark" "Parseo, is_risky, agregación por ventana y 2 alertas"
crear "Persistencia en Parquet con checkpoint" "semana-2,data-lake" "Capas raw, processed y curated"
crear "Pruebas con pytest y workflow ci.yml" "semana-2,tests-ci" "compileall, pytest, contrato y docker compose config"
crear "DAG de Airflow con minimo 5 tasks" "semana-3,airflow" "Dependencias, retries y XCom"
crear "Fallo controlado y recuperación documentada" "semana-3,calidad" "Provocar un fallo, registrar el log real y la recuperación"
crear "Quality gate, README final, video y release v1.0.0" "semana-4,release" "CI verde, README completo, video <= 10 min y tag v1.0.0"

echo "Issues creados. Asignen responsables desde GitHub o con: gh issue edit <n> --add-assignee <usuario>"
