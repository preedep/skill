use crate::model::{Job, Variable};

#[derive(Debug, Clone)]
pub enum OperatorKind {
    SshOperator { conn_id: String, command: String },
    PsrpOperator { conn_id: String, command: String },
    FileSensor { filepath: String, timeout: u64, poke_interval: u64 },
    StepFunction { aws_conn_id: String, state_machine_arn: String, execution_name: String, payload: String },
    Todo { reason: String },
}

pub fn map_operator(job: &Job) -> OperatorKind {
    match job.appl_type.as_str() {
        "OS" => map_os(job),
        "FileWatch" => map_filewatch(job),
        "FILE_TRANS" => map_file_trans(job),
        "AWS" => map_aws(job),
        other => OperatorKind::Todo {
            reason: format!("APPL_TYPE={} is not yet supported", other),
        },
    }
}

fn map_os(job: &Job) -> OperatorKind {
    let conn_id = conn_id_from_nodeid(&job.nodeid);
    let command = crate::substitution::translate(&job.cmdline);
    if is_windows_node(&job.nodeid) {
        OperatorKind::PsrpOperator { conn_id: conn_id_psrp(&job.nodeid), command }
    } else {
        OperatorKind::SshOperator { conn_id, command }
    }
}

fn map_filewatch(job: &Job) -> OperatorKind {
    let filepath = get_var(&job.variables, "%%FileWatch-FILE_PATH")
        .or_else(|| get_var(&job.variables, "%%FileWatch-FILEPATH"))
        .unwrap_or_default();
    let filepath = crate::substitution::translate(&filepath)
        .replace('\\', "/");

    let time_limit: u64 = get_var(&job.variables, "%%FileWatch-TIME_LIMIT")
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
        * 60; // convert minutes to seconds

    let num_iter: u64 = get_var(&job.variables, "%%FileWatch-NUM_OF_ITERATIONS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    let poke_interval = if num_iter > 0 { time_limit / num_iter } else { 60 };

    OperatorKind::FileSensor { filepath, timeout: time_limit, poke_interval }
}

fn map_file_trans(job: &Job) -> OperatorKind {
    let conn_type2 = get_var(&job.variables, "FTP-CONNTYPE2").unwrap_or_default();

    if conn_type2.to_uppercase() == "S3" {
        let bucket = get_var(&job.variables, "FTP-S3_BUCKET_NAME").unwrap_or_default();
        let lpath = get_var(&job.variables, "FTP-LPATH1").unwrap_or_default();
        let rpath = get_var(&job.variables, "FTP-RPATH1").unwrap_or_default();
        let upload = get_var(&job.variables, "FTP-UPLOAD1").unwrap_or_default();
        let command = if upload == "1" {
            format!(
                "aws s3 cp {} s3://{}{}",
                crate::substitution::translate(&lpath),
                bucket,
                crate::substitution::translate(&rpath)
            )
        } else {
            format!(
                "aws s3 cp s3://{}{} {}",
                bucket,
                crate::substitution::translate(&rpath),
                crate::substitution::translate(&lpath)
            )
        };
        OperatorKind::SshOperator {
            conn_id: conn_id_from_nodeid(&job.nodeid),
            command,
        }
    } else {
        let lostype = get_var(&job.variables, "FTP-LOSTYPE")
            .or_else(|| get_var(&job.variables, "FTP_LOSTYPE"))
            .unwrap_or_default();
        let lpath = get_var(&job.variables, "FTP-LPATH1").unwrap_or_default();
        let rhost = get_var(&job.variables, "FTP-RHOST").unwrap_or_default();
        let rpath = get_var(&job.variables, "FTP-RPATH1").unwrap_or_default();
        let command = format!(
            "lftp -c 'open {}; lcd {}; mget {}'",
            rhost,
            crate::substitution::translate(&lpath),
            crate::substitution::translate(&rpath)
        );
        if lostype.to_uppercase().contains("WIN") {
            OperatorKind::PsrpOperator {
                conn_id: conn_id_psrp(&job.nodeid),
                command,
            }
        } else {
            OperatorKind::SshOperator {
                conn_id: conn_id_from_nodeid(&job.nodeid),
                command,
            }
        }
    }
}

fn map_aws(job: &Job) -> OperatorKind {
    let service_type = get_var(&job.variables, "AWS-SERVICE_TYPE")
        .or_else(|| get_var(&job.variables, "%%AWS-SERVICE_TYPE"))
        .unwrap_or_default();

    if service_type.to_uppercase() == "STEP" {
        let step_name = get_var(&job.variables, "AWS-STEP_NAME")
            .or_else(|| get_var(&job.variables, "%%AWS-STEP_NAME"))
            .unwrap_or_default();
        let account = get_var(&job.variables, "AWS-ACCOUNT")
            .or_else(|| get_var(&job.variables, "%%AWS-ACCOUNT"))
            .unwrap_or_default();
        let exec_name = get_var(&job.variables, "AWS-STEP_EXECUTION_NAME")
            .or_else(|| get_var(&job.variables, "%%AWS-STEP_EXECUTION_NAME"))
            .unwrap_or_else(|| job.jobname.to_lowercase());
        let payload = get_var(&job.variables, "AWS-STEP_PAYLOAD_JSON-N001-VALUE")
            .or_else(|| get_var(&job.variables, "%%AWS-STEP_PAYLOAD_JSON-N001-VALUE"))
            .map(|v| crate::substitution::translate(&v))
            .unwrap_or_else(|| "{}".to_string());

        let aws_conn_id = format!("aws_{}", account.to_lowercase());
        let state_machine_arn = format!(
            "arn:aws:states:ap-southeast-1:ACCOUNT_ID_PLACEHOLDER:stateMachine:{}",
            step_name
        );
        let execution_name = format!("{}-{{{{ ts_nodash }}}}", exec_name);

        OperatorKind::StepFunction {
            aws_conn_id,
            state_machine_arn,
            execution_name,
            payload,
        }
    } else {
        OperatorKind::Todo {
            reason: format!("AWS SERVICE_TYPE={} is not yet supported", service_type),
        }
    }
}

fn get_var(vars: &[Variable], name: &str) -> Option<String> {
    vars.iter()
        .find(|v| v.name.eq_ignore_ascii_case(name))
        .map(|v| v.value.clone())
}

fn conn_id_from_nodeid(nodeid: &str) -> String {
    format!("ssh_{}", nodeid.to_lowercase())
}

fn conn_id_psrp(nodeid: &str) -> String {
    format!("psrp_{}", nodeid.to_lowercase())
}

fn is_windows_node(nodeid: &str) -> bool {
    let n = nodeid.to_lowercase();
    n.contains("win") || n.contains("winsrv") || n.contains("psrp")
}
