use anyhow::Result;
use minijinja::Environment;
use std::collections::HashMap;

use crate::dependencies::{resolve, DependencyResult};
use crate::mapper::{map_operator, OperatorKind};
use crate::model::{Folder, Job};
use crate::scheduler::{derive_execution_period, derive_schedule};

const DAG_TEMPLATE: &str = r#"# Apache Airflow DAG: {{ company }}-{{ app_id }}-{{ app_code }}-{{ folder_name_lower }}-{{ env }}
# Converted from Control-M folder: {{ folder_name_upper }}
# Converted by Control-M 2 Airflow Skill

from airflow import DAG

from datetime import datetime, timedelta
import logging
import pendulum
{% if has_step_function %}
import json
{% endif %}

from airflow.utils.email import send_email
from airflow.providers.standard.operators.empty import EmptyOperator
{% if has_ssh %}
from airflow.providers.ssh.operators.ssh import SSHOperator
{% endif %}
{% if has_psrp %}
from airflow.providers.microsoft.psrp.operators.psrp import PsrpOperator
{% endif %}
{% if has_filesensor %}
from airflow.providers.standard.sensors.filesystem import FileSensor
{% endif %}
{% if has_step_function %}
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator
from airflow.providers.amazon.aws.sensors.step_function import StepFunctionExecutionSensor
{% endif %}
{% if has_external_sensor %}
from airflow.sensors.external_task import ExternalTaskSensor
{% endif %}
{% if has_external_sensor or has_or_gate %}
from airflow.utils.trigger_rule import TriggerRule
{% endif %}

###################### logging ######################

logging.getLogger("smtplib").setLevel(logging.DEBUG)
logging.getLogger("airflow.utils.email").setLevel(logging.DEBUG)

###################### variables zone ######################

_company = "{{ company }}"
_project = "{{ app_id }}"
_app_code = "{{ app_code }}"
_env = "{{ env }}"
_dag_name = "{{ folder_name_lower }}"

_active = True
_schedule = {{ schedule }}
_tags = {{ tags }}
_email_list = []
_enable_email_notification_success = False
_enable_email_notification_fail = False

################### Callbacks ####################

def success_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = pendulum.now('Asia/Bangkok')
    subject = f"DAG {dag_id} - Task {task_id} succeeded"
    body = f"""
    <h3>Task Succeeded</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Task:</strong> {task_id}</p>
    <p><strong>Execution Time:</strong> {execution_date}</p>
    """
    if _enable_email_notification_success:
        send_email(to=_email_list, subject=subject, html_content=body)

def failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = pendulum.now('Asia/Bangkok')
    exception = context.get('exception', 'Unknown error')
    subject = f"DAG {dag_id} - Task {task_id} failed"
    body = f"""
    <h3>Task Failed</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Task:</strong> {task_id}</p>
    <p><strong>Execution Time:</strong> {execution_date}</p>
    <p><strong>Error:</strong> {exception}</p>
    """
    if _enable_email_notification_fail:
        send_email(to=_email_list, subject=subject, html_content=body)

################### DAG Configuration ####################

local_tz = pendulum.timezone('Asia/Bangkok')

default_args = {
    'owner': '{}'.format(_project),
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1, tzinfo=local_tz),
    'timezone': 'Asia/Bangkok',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    _company + '-' + _project + '-' + _app_code + '-' + _dag_name + '-' + _env,
    default_args=default_args,
    schedule=_schedule,
    tags=_tags,
    catchup=False,
    is_paused_upon_creation=not _active,
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    on_success_callback=success_callback if _enable_email_notification_success else None,
    on_failure_callback=failure_callback if _enable_email_notification_fail else None,
) as dag:

    ###################### Tasks ######################

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

{{ task_blocks }}

    ###################### Task Dependencies ######################

{{ dependency_block }}
"#;

pub struct RenderContext {
    pub company: String,
    pub app_id: String,
    pub app_code: String,
    pub env: String,
    /// Maps JOB_NAME_UPPERCASE -> folder_name for external DAG ID derivation
    pub job_folder_map: HashMap<String, String>,
}

pub fn render(folder: &Folder, ctx: &RenderContext) -> Result<String> {
    let schedule_result = derive_schedule(folder);
    let period = derive_execution_period(&folder.folder_name);
    let deps = resolve(folder);

    let folder_lower = folder.folder_name.to_lowercase();
    let tags = format!(
        "[\"{}\", \"{}\", \"{}\", \"{}\", \"{}\"]",
        ctx.company.to_lowercase(),
        ctx.app_id.to_lowercase(),
        ctx.app_code.to_lowercase(),
        folder_lower,
        ctx.env.to_lowercase()
    );

    // Collect operator types and gate flags
    let mut has_ssh = false;
    let mut has_psrp = false;
    let mut has_filesensor = false;
    let mut has_step_function = false;
    let mut has_external_sensor = false;
    let has_or_gate = deps.iter().any(|(_, dep)| dep.has_or_gate);

    for (_, dep) in &deps {
        if !dep.external.is_empty() {
            has_external_sensor = true;
        }
    }

    let mut task_blocks = Vec::new();
    for job in &folder.jobs {
        let op = map_operator(job);
        match &op {
            OperatorKind::SshOperator { .. } => has_ssh = true,
            OperatorKind::PsrpOperator { .. } => has_psrp = true,
            OperatorKind::FileSensor { .. } => has_filesensor = true,
            OperatorKind::StepFunction { .. } => has_step_function = true,
            _ => {}
        }
        task_blocks.push(render_task(job, &op, ctx, period));
    }

    let dep_block = render_dependencies(folder, &deps, ctx, period);

    let mut schedule = schedule_result.schedule.clone();
    if let Some(todo) = &schedule_result.todo {
        schedule = format!("{}  # {}", schedule, todo);
    }

    let mut env = Environment::new();
    env.add_template("dag", DAG_TEMPLATE)?;
    let tmpl = env.get_template("dag")?;

    let output = tmpl.render(minijinja::context! {
        company => ctx.company.to_lowercase(),
        app_id => ctx.app_id,
        app_code => ctx.app_code,
        env => ctx.env.to_lowercase(),
        folder_name_lower => folder_lower,
        folder_name_upper => folder.folder_name.clone(),
        schedule => schedule,
        tags => tags,
        has_ssh => has_ssh,
        has_psrp => has_psrp,
        has_filesensor => has_filesensor,
        has_step_function => has_step_function,
        has_external_sensor => has_external_sensor,
        has_or_gate => has_or_gate,
        task_blocks => task_blocks.join("\n"),
        dependency_block => dep_block,
    })?;

    Ok(output)
}

fn task_var_name(job: &Job, ctx: &RenderContext, period: &str) -> String {
    format!(
        "{}_{}_task_{}_{}",
        ctx.app_id.to_lowercase().replace('-', "_"),
        ctx.app_code.to_lowercase().replace('-', "_"),
        job.jobname.to_lowercase().replace('-', "_"),
        period
    )
    .replace("__", "_")
}

fn task_id_str(job: &Job, ctx: &RenderContext, period: &str) -> String {
    format!(
        "{}-{}-task_{}-{}",
        ctx.app_id.to_lowercase(),
        ctx.app_code.to_lowercase(),
        job.jobname.to_lowercase(),
        period
    )
}

fn render_task(job: &Job, op: &OperatorKind, ctx: &RenderContext, period: &str) -> String {
    let var_name = task_var_name(job, ctx, period);
    let task_id = task_id_str(job, ctx, period);

    let ctrlm_comment = format!(
        "    # Control-M job: {}\n\
         # INCOND: {} | OUTCOND: {}\n\
         # RUN_AS: {}\n\
         # JOBNAME: {} | JOBISN: {} | NODEID: {} | APPL_TYPE: {} | APPL_FORM: {}\n\
         # TIMEFROM: {} | CYCLIC: {} | INTERVAL: {} | DAYSCAL: {} | CRITICAL: {}\n\
         # PARENT_FOLDER: {} | APPLICATION: {} | SUB_APPLICATION: {}",
        job.jobname,
        job.inconds.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(", "),
        job.outconds.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(", "),
        job.run_as,
        job.jobname, job.jobisn, job.nodeid, job.appl_type, job.appl_form,
        job.timefrom, job.cyclic, job.interval,
        job.dayscal, job.critical,
        job.parent_folder, job.application, job.sub_application
    );

    let operator_block = match op {
        OperatorKind::SshOperator { conn_id, command } => format!(
            r#"    {var_name} = SSHOperator(
        task_id='{task_id}',
        ssh_conn_id='{conn_id}',
        command=r"""{command}""",
        cmd_timeout=3600,
        on_failure_callback=failure_callback if _enable_email_notification_fail else None,
    )"#
        ),
        OperatorKind::PsrpOperator { conn_id, command } => format!(
            r#"    {var_name} = PsrpOperator(
        task_id='{task_id}',
        psrp_conn_id='{conn_id}',
        # RUN_AS: {run_as}
        powershell=r"""{command}""",
        wsman_options={{"ssl": False}},
        on_failure_callback=failure_callback if _enable_email_notification_fail else None,
    )"#,
            run_as = job.run_as,
        ),
        OperatorKind::FileSensor { filepath, timeout, poke_interval } => format!(
            r#"    {var_name} = FileSensor(
        task_id='{task_id}',
        filepath='{filepath}',
        mode='reschedule',
        poke_interval={poke_interval},
        timeout={timeout},
        on_failure_callback=failure_callback if _enable_email_notification_fail else None,
    )"#
        ),
        OperatorKind::StepFunction { aws_conn_id, state_machine_arn, execution_name, payload } => {
            let wait_var = format!("{}_wait", var_name);
            let wait_task_id = format!("{}-wait", task_id);
            format!(
                r#"    {var_name} = StepFunctionStartExecutionOperator(
        task_id='{task_id}',
        aws_conn_id='{aws_conn_id}',
        state_machine_arn='{state_machine_arn}',  # TODO: replace ACCOUNT_ID_PLACEHOLDER
        name='{execution_name}',
        input=json.dumps({payload}),
        on_failure_callback=failure_callback if _enable_email_notification_fail else None,
    )
    {wait_var} = StepFunctionExecutionSensor(
        task_id='{wait_task_id}',
        aws_conn_id='{aws_conn_id}',
        execution_arn="{{{{ ti.xcom_pull(task_ids='{task_id}') }}}}",
        poke_interval=30,
        timeout=3600,
        mode='reschedule',
        on_failure_callback=failure_callback if _enable_email_notification_fail else None,
    )"#
            )
        }
        OperatorKind::Todo { reason } => format!(
            r#"    # TODO: {reason}
    {var_name} = EmptyOperator(
        task_id='{task_id}',
    )"#
        ),
    };

    format!("{ctrlm_comment}\n{operator_block}")
}

fn render_dependencies(
    folder: &Folder,
    deps: &[(String, DependencyResult)],
    ctx: &RenderContext,
    period: &str,
) -> String {
    let mut lines = Vec::new();
    let mut jobs_with_deps: std::collections::HashSet<String> = std::collections::HashSet::new();

    for (job_name, dep) in deps {
        let job = folder.jobs.iter().find(|j| j.jobname == *job_name).unwrap();
        let successor_var = task_var_name(job, ctx, period);

        // External sensors
        for ext in &dep.external {
            let sensor_var = format!(
                "sensor_{}_{}",
                ext.job_name.to_lowercase().replace('-', "_"),
                period
            );
            let sensor_task_id = format!(
                "wait-ext-{}-{}",
                ext.job_name.to_lowercase(),
                period
            );
            let trigger_rule = if dep.has_or_gate {
                "\n        trigger_rule=TriggerRule.ONE_SUCCESS,"
            } else {
                ""
            };
            let external_dag_id = ctx.job_folder_map
                .get(&ext.job_name.to_uppercase())
                .map(|f| format!("{}-{}-{}-{}-{}",
                    ctx.company.to_lowercase(),
                    ctx.app_id.to_lowercase(),
                    ctx.app_code.to_lowercase(),
                    f.to_lowercase(),
                    ctx.env.to_lowercase()))
                .unwrap_or_else(|| format!("TODO_external_dag_for_{}", ext.job_name.to_lowercase()));

            let external_task_id = ctx.job_folder_map
                .get(&ext.job_name.to_uppercase())
                .map(|_| format!("{}-{}-task_{}-{}",
                    ctx.app_id.to_lowercase(),
                    ctx.app_code.to_lowercase(),
                    ext.job_name.to_lowercase(),
                    period))
                .unwrap_or_else(|| format!("TODO_task_id_for_{}", ext.job_name.to_lowercase()));

            lines.push(format!(
                r#"    {sensor_var} = ExternalTaskSensor(
        task_id='{sensor_task_id}',
        external_dag_id='{external_dag_id}',
        external_task_id='{external_task_id}',{trigger_rule}
        mode='reschedule',
    )
    {sensor_var} >> {successor_var}"#
            ));
            jobs_with_deps.insert(job_name.clone());
        }

        // Internal dependencies
        for internal in &dep.internal {
            let pred_job = folder.jobs.iter().find(|j| {
                j.jobname.to_uppercase() == internal.predecessor.to_uppercase()
            });
            if let Some(pred) = pred_job {
                let pred_var = task_var_name(pred, ctx, period);
                if dep.has_or_gate {
                    lines.push(format!(
                        "    {successor_var}.set_upstream({pred_var}, trigger_rule=TriggerRule.ONE_SUCCESS)"
                    ));
                } else {
                    lines.push(format!("    {pred_var} >> {successor_var}"));
                }
                jobs_with_deps.insert(job_name.clone());
                jobs_with_deps.insert(internal.predecessor.clone());
            }
        }
    }

    // Wire independent tasks through start/end
    let independent: Vec<String> = folder
        .jobs
        .iter()
        .filter(|j| !jobs_with_deps.contains(&j.jobname))
        .map(|j| task_var_name(j, ctx, period))
        .collect();

    let all_tasks: Vec<String> = folder
        .jobs
        .iter()
        .map(|j| task_var_name(j, ctx, period))
        .collect();

    if !independent.is_empty() {
        lines.push(format!(
            "    start >> [{}] >> end",
            independent.join(", ")
        ));
    } else if !all_tasks.is_empty() {
        lines.push(format!(
            "    start >> [{}] >> end",
            all_tasks.join(", ")
        ));
    }

    if lines.is_empty() {
        "    start >> end".to_string()
    } else {
        lines.join("\n")
    }
}
