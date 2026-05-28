use std::collections::HashSet;
use crate::model::{Folder, Job};

pub struct TaskDep {
    pub predecessor: String,
    #[allow(dead_code)]
    pub successor: String,
}

pub struct ExternalDep {
    pub job_name: String,
    #[allow(dead_code)]
    pub condition_name: String,
    #[allow(dead_code)]
    pub and_or: String,
}

pub struct DependencyResult {
    pub internal: Vec<TaskDep>,
    pub external: Vec<ExternalDep>,
    pub has_or_gate: bool,
}

const STATUS_SUFFIXES: &[&str] = &[
    "-ENDED-OK", "-ENDED", "-END-OK", "-ENED-OK",
    "-RERUN", "-M2F", "-SAT", "-SUN", "-SPECIFIC",
];

pub fn resolve(folder: &Folder) -> Vec<(String, DependencyResult)> {
    let job_names: HashSet<String> = folder
        .jobs
        .iter()
        .map(|j| j.jobname.to_uppercase())
        .collect();

    folder
        .jobs
        .iter()
        .map(|job| {
            let result = resolve_job(job, &job_names);
            (job.jobname.clone(), result)
        })
        .collect()
}

fn resolve_job(job: &Job, folder_jobs: &HashSet<String>) -> DependencyResult {
    let mut internal = Vec::new();
    let mut external = Vec::new();
    let mut has_or_gate = false;

    for incond in &job.inconds {
        if incond.and_or == "O" {
            has_or_gate = true;
        }

        let predecessor_name = extract_job_name(&incond.name);

        if folder_jobs.contains(&predecessor_name.to_uppercase()) {
            internal.push(TaskDep {
                predecessor: predecessor_name.to_string(),
                successor: job.jobname.clone(),
            });
        } else {
            external.push(ExternalDep {
                job_name: predecessor_name.to_string(),
                condition_name: incond.name.clone(),
                and_or: incond.and_or.clone(),
            });
        }
    }

    DependencyResult { internal, external, has_or_gate }
}

fn extract_job_name(condition: &str) -> &str {
    // Check numbered variant first: NAME-ENDED-OK-123
    let numbered_re = regex::Regex::new(r"^(.+)-ENDED-OK-\d+$").unwrap();
    if let Some(caps) = numbered_re.captures(condition) {
        let end = caps.get(1).unwrap().end();
        return &condition[..end];
    }

    // Strip known status suffixes
    for suffix in STATUS_SUFFIXES {
        if let Some(stripped) = condition.strip_suffix(suffix) {
            return stripped;
        }
    }

    // Non-standard token — return verbatim (preserve as-is)
    condition
}
