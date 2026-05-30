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
    let filepath_raw = get_var(&job.variables, "FileWatch-FILE_PATH")
        .or_else(|| get_var(&job.variables, "FileWatch-FILEPATH"))
        .unwrap_or_default();

    let time_limit_min: u64 = get_var(&job.variables, "FileWatch-TIME_LIMIT")
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let time_limit_sec = time_limit_min * 60;

    let num_iter: u64 = get_var(&job.variables, "FileWatch-NUM_OF_ITERATIONS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let int_filesize: u64 = get_var(&job.variables, "FileWatch-INT_FILESIZE_COMPARISON")
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    let poke_interval: u64 = get_var(&job.variables, "FileWatch-INT_FILE_SEARCHES")
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| if num_iter > 0 { time_limit_sec / num_iter } else { 60 });

    // Windows drive path (e.g. S:\, D:\) or Windows NODEID -> PsrpOperator polling
    let is_windows_path = filepath_raw.len() >= 2
        && filepath_raw.chars().next().map(|c| c.is_ascii_alphabetic()).unwrap_or(false)
        && filepath_raw.chars().nth(1) == Some(':');

    if is_windows_path || is_windows_node(&job.nodeid) {
        let filepath_translated = crate::substitution::translate(&filepath_raw);
        let conn_id = conn_id_psrp(&job.nodeid);
        let poll_sec = if num_iter > 0 { time_limit_sec / num_iter } else { poke_interval };
        let command = format!(
            "# TODO: implement Windows remote file watch\n\
             # Replace this PsrpOperator polling placeholder with a proper custom sensor when available.\n\
             # This task polls every {poll_sec}s up to {time_limit_sec}s for the file to appear.\n\
             # Stability check: {num_iter} iterations x {int_filesize}s size comparison window.\n\
             $ErrorActionPreference = 'Stop'\n\
             $FilePath = \"{filepath_translated}\"\n\
             $TimeoutSec = {time_limit_sec}\n\
             $PollSec = {poll_sec}\n\
             $Elapsed = 0\n\
             Write-Host \"[INFO] Waiting for file: $FilePath\"\n\
             while (-not (Test-Path $FilePath)) {{\n\
                 if ($Elapsed -ge $TimeoutSec) {{\n\
                     Write-Host \"[ERROR] File not found after ${{TimeoutSec}}s: $FilePath\"\n\
                     exit 1\n\
                 }}\n\
                 Write-Host \"[INFO] File not yet present. Elapsed: ${{Elapsed}}s / ${{TimeoutSec}}s\"\n\
                 Start-Sleep -Seconds $PollSec\n\
                 $Elapsed += $PollSec\n\
             }}\n\
             Write-Host \"[INFO] File found: $FilePath\"\n             "
        );
        return OperatorKind::PsrpOperator { conn_id, command };
    }

    // Unix path -- FileSensor (local worker) or SFTPSensor (remote Unix)
    let filepath = crate::substitution::translate(&filepath_raw);
    OperatorKind::FileSensor { filepath, timeout: time_limit_sec, poke_interval }
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
        let upload = get_var(&job.variables, "FTP-UPLOAD1").unwrap_or_default();

        let lpath_t = crate::substitution::translate(&lpath);
        let rpath_t = crate::substitution::translate(&rpath);

        let command = if upload == "1" {
            format!(
                "lftp -c 'open {}; lcd {}; mput {}'",
                rhost, lpath_t, rpath_t
            )
        } else {
            format!(
                "lftp -c 'open {}; lcd {}; mget {}'",
                rhost, lpath_t, rpath_t
            )
        };

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
        .find(|v| {
            let stored = v.name.trim_start_matches('%');
            stored.eq_ignore_ascii_case(name) || v.name.eq_ignore_ascii_case(name)
        })
        .map(|v| v.value.clone())
}

fn conn_id_from_nodeid(nodeid: &str) -> String {
    format!("ssh_{}", nodeid.to_lowercase())
}

fn conn_id_psrp(nodeid: &str) -> String {
    format!("psrp_{}", nodeid.to_lowercase())
}

// Node ID table: Glory=Windows, Dunlop=Linux, Donut=Linux
// Also matches generic win/winsrv/psrp naming conventions
fn is_windows_node(nodeid: &str) -> bool {
    let n = nodeid.to_lowercase();
    n == "glory" || n.contains("win") || n.contains("winsrv") || n.contains("psrp")
}
