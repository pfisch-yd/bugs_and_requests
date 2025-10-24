🎯 Solução Proposta: Arquitetura de Processamento Assíncrono com Freeport
📋 Resumo da Solução
Separar o processamento principal (run_everything_parallelized) do processamento Freeport usando jobs assíncronos independentes com orquestração via Databricks Workflows ou Airflow.
🏗️ Arquitetura em Alto Nível
┌─────────────────────────────────────────────────────────────┐
│  JOB PRINCIPAL (Databricks Job)                             │
│  run_everything_parallelized("testblueprints")              │
└─────────────────────────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   ┌─────────┐     ┌─────────┐     ┌─────────┐
   │run_geo  │     │run_pro  │     │run_market│
   │_analysis│     │_insights│     │_share   │
   └─────────┘     └─────────┘     └─────────┘
        │                │                │
        │ TRIGGER        │ TRIGGER        │ TRIGGER
        ▼                ▼                ▼
   ┌─────────┐     ┌─────────┐     ┌─────────┐
   │Freeport │     │Freeport │     │Freeport │
   │Geo Job  │     │Pro Job  │     │Market Job│
   │(async)  │     │(async)  │     │(async)  │
   └─────────┘     └─────────┘     └─────────┘
        │                │                │
        └────────────────┴────────────────┘
                         │
                         ▼
              ┌──────────────────┐
              │ Email Notification│
              │ "All Freeport     │
              │  jobs complete!"  │
              └──────────────────┘
🔧 Componentes da Solução
1. Job Principal (run_everything_parallelized)
Responsabilidade: Criar/atualizar as TABLES normalmente Modificação necessária:
Após cada run_module completar, trigger um job Freeport separado
NÃO esperar o Freeport terminar
Retornar sucesso assim que todas as tables forem criadas
def run_everything_parallelized_with_freeport_triggers(demo_name):
    # ... código existente ...
    
    # Ao final de cada run_module:
    if module_succeeded:
        trigger_freeport_job_async(
            module_name="geo_analysis",
            table_name=f"{demo_name}_geo_analysis",
            demo_name=demo_name
        )
    
    # Job principal retorna sucesso IMEDIATAMENTE
    return {"status": "success", "freeport_jobs_triggered": True}
2. Função de Trigger Assíncrono
Opção A: Databricks Jobs API
from databricks.sdk import WorkspaceClient

def trigger_freeport_job_async(module_name, table_name, demo_name):
    """
    Trigger um Databricks Job separado para processar Freeport
    """
    w = WorkspaceClient()
    
    # Trigger job específico para este módulo
    run = w.jobs.run_now(
        job_id=FREEPORT_JOB_IDS[module_name],  # Job ID pré-configurado
        notebook_params={
            "module_name": module_name,
            "table_name": table_name,
            "demo_name": demo_name
        }
    )
    
    # Salvar run_id em tabela de tracking
    log_freeport_job(demo_name, module_name, run.run_id)
    
    print(f"✅ Freeport job triggered for {module_name} (run_id: {run.run_id})")
    # NÃO esperar o job terminar - retorna imediatamente
Opção B: Delta Live Tables / Event-Driven
# Escrever evento em uma "freeport_queue" table
def enqueue_freeport_job(module_name, table_name, demo_name):
    """
    Adiciona job na fila de processamento Freeport
    """
    queue_entry = spark.createDataFrame([{
        "demo_name": demo_name,
        "module_name": module_name,
        "table_name": table_name,
        "status": "pending",
        "created_at": datetime.now(),
        "run_id": None
    }])
    
    queue_entry.write.mode("append").saveAsTable("freeport_job_queue")
    
    # Um Databricks Job separado monitora essa tabela e processa a fila
3. Jobs Freeport Independentes
Criar um Databricks Job para cada módulo (ou um job genérico): Notebook: freeport_processor.py
# Databricks notebook source
# Recebe parâmetros via widgets
dbutils.widgets.text("module_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("demo_name", "")

module_name = dbutils.widgets.get("module_name")
table_name = dbutils.widgets.get("table_name")
demo_name = dbutils.widgets.get("demo_name")

# COMMAND ----------

# Executar rotina Freeport
print(f"🔄 Starting Freeport processing for {module_name}...")

deliverable, materialization = freeport_geo_analysis(
    sandbox_schema="ydx_internal_analysts_sandbox",
    prod_schema="ydx_internal_analysts_gold",
    demo_name=demo_name,
    module_name=module_name
)

materialization_id = materialization['id']
release_materialization(materialization_id)

# Esperar release completar
wait_for_freeport_release(materialization_id)

print(f"✅ Freeport processing complete for {module_name}")

# COMMAND ----------

# Atualizar status no tracking table
update_freeport_status(demo_name, module_name, "completed")
4. Tabela de Tracking
Criar uma tabela para rastrear status dos jobs Freeport:
CREATE TABLE IF NOT EXISTS freeport_job_tracking (
    demo_name STRING,
    module_name STRING,
    table_name STRING,
    run_id STRING,
    status STRING,  -- pending, running, completed, failed
    triggered_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message STRING
)
5. Monitoramento e Notificação
Opção A: Databricks Job com Schedule Criar um job que roda a cada 5 minutos:
# check_freeport_completion.py

def check_all_freeport_jobs_complete(demo_name):
    """
    Verifica se todos os jobs Freeport foram completados
    """
    df = spark.sql(f"""
        SELECT 
            COUNT(*) as total_jobs,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_jobs,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_jobs
        FROM freeport_job_tracking
        WHERE demo_name = '{demo_name}'
        AND triggered_at >= current_date()
    """).collect()[0]
    
    if df.total_jobs == df.completed_jobs + df.failed_jobs:
        # Todos completaram (sucesso ou falha)
        send_completion_email(demo_name, df.completed_jobs, df.failed_jobs)
        return True
    
    return False

def send_completion_email(demo_name, completed, failed):
    """
    Envia email notificando conclusão
    """
    subject = f"Freeport Processing Complete: {demo_name}"
    body = f"""
    ✅ All Freeport jobs have completed for {demo_name}
    
    Summary:
    - Completed: {completed}
    - Failed: {failed}
    
    Check details at: https://your-databricks-workspace.com/...
    """
    
    # Usar Databricks email notification ou API externa
    send_email(subject, body, recipients=["team@company.com"])
Opção B: Databricks Workflow com Depends On
# Databricks Multi-Task Job Configuration
tasks:
  - task_key: main_processing
    notebook_task:
      notebook_path: /run_everything_parallelized
    
  - task_key: freeport_geo
    depends_on: [main_processing]
    notebook_task:
      notebook_path: /freeport_processor
      base_parameters:
        module_name: geo_analysis
    
  - task_key: freeport_pro
    depends_on: [main_processing]
    notebook_task:
      notebook_path: /freeport_processor
      base_parameters:
        module_name: pro_insights
  
  - task_key: notify
    depends_on: [freeport_geo, freeport_pro, ...]
    notebook_task:
      notebook_path: /send_notification
🎯 Recomendação Final
Abordagem Híbrida (Melhor das duas)
Job Principal usa trigger_freeport_job_async() para engatilhar jobs separados
Jobs Freeport rodam independentemente em paralelo
Tracking Table mantém estado de todos os jobs
Monitoring Job (scheduled a cada 5 min) verifica conclusão e envia email
Vantagens:
✅ Job principal termina rápido (~tempo normal)
✅ Freeport processa em paralelo (10min cada, não 10min × N)
✅ Falha em um Freeport não afeta outros
✅ Rastreabilidade completa
✅ Notificação automática quando tudo terminar
Implementação Incremental:
Fase 1: Adicionar trigger assíncrono no código existente
Fase 2: Criar jobs Freeport separados
Fase 3: Adicionar tracking table
Fase 4: Implementar monitoramento e notificações
Quer que eu implemente alguma dessas partes em código?