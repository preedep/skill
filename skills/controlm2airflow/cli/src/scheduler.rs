use crate::model::Folder;

pub struct ScheduleResult {
    pub schedule: String,
    pub todo: Option<String>,
}

pub fn derive_schedule(folder: &Folder) -> ScheduleResult {
    // Custom calendar — cannot express as cron
    if !folder.dayscal.is_empty() || !folder.confcal.is_empty() {
        return ScheduleResult {
            schedule: "None".to_string(),
            todo: Some("TODO: implement custom Timetable (DAYSCAL/CONFCAL present)".to_string()),
        };
    }

    // Manual trigger
    if folder.folder_order_method.is_empty() || folder.folder_order_method == "MANUAL" {
        return ScheduleResult {
            schedule: "None".to_string(),
            todo: Some("TODO: set schedule (manual trigger folder)".to_string()),
        };
    }

    // Derive from folder name suffix + TIMEFROM
    let name_upper = folder.folder_name.to_uppercase();

    // Find the TIMEFROM from the first job in the folder
    let timefrom = folder
        .jobs
        .iter()
        .find(|j| !j.timefrom.is_empty())
        .map(|j| j.timefrom.as_str())
        .unwrap_or("");

    let (hh, mm) = parse_timefrom(timefrom);

    // Check cyclic interval from first job
    if let Some(job) = folder.jobs.first() {
        if job.cyclic == "1" && !job.interval.is_empty() {
            if let Some(cron) = cyclic_to_cron(&job.interval, hh, mm) {
                return ScheduleResult { schedule: format!("\"{}\"", cron), todo: None };
            }
        }
    }

    if name_upper.contains("_MONTHLY") || name_upper.contains("_MTH") {
        return ScheduleResult {
            schedule: format!("\"{} {} 1 * *\"", mm, hh),
            todo: None,
        };
    }
    if name_upper.contains("_WEEKLY") || name_upper.contains("_WKL") {
        return ScheduleResult {
            schedule: format!("\"{} {} * * 0\"", mm, hh),
            todo: None,
        };
    }
    if name_upper.contains("_DAILY") || name_upper.contains("_DLY") {
        return ScheduleResult {
            schedule: format!("\"{} {} * * *\"", mm, hh),
            todo: None,
        };
    }
    if name_upper.contains("_YEARLY") || name_upper.contains("_ANNUAL") {
        return ScheduleResult {
            schedule: format!("\"{} {} 1 1 *\"", mm, hh),
            todo: None,
        };
    }

    // Default: daily with TIMEFROM if available
    if !timefrom.is_empty() {
        return ScheduleResult {
            schedule: format!("\"{} {} * * *\"", mm, hh),
            todo: Some("TODO: verify schedule — derived from TIMEFROM, no folder suffix matched".to_string()),
        };
    }

    ScheduleResult {
        schedule: "None".to_string(),
        todo: Some("TODO: set schedule".to_string()),
    }
}

fn parse_timefrom(timefrom: &str) -> (&str, &str) {
    if timefrom.len() == 4 {
        (&timefrom[0..2], &timefrom[2..4])
    } else {
        ("0", "0")
    }
}

fn cyclic_to_cron(interval: &str, hh: &str, mm: &str) -> Option<String> {
    // Format: 00060M = 60 minutes, 00001H = 1 hour
    let digits: String = interval.chars().take_while(|c| c.is_ascii_digit()).collect();
    let unit = interval.chars().last()?;
    let value: u32 = digits.trim_start_matches('0').parse().unwrap_or(0);

    match unit {
        'M' => {
            if value == 60 {
                Some("@hourly".to_string())
            } else if value > 0 && 60 % value == 0 {
                Some(format!("*/{} * * * *", value))
            } else {
                Some(format!("*/{} * * * *", value))
            }
        }
        'H' => {
            if value == 1 {
                Some("@hourly".to_string())
            } else {
                Some(format!("0 */{} * * *", value))
            }
        }
        'D' => Some(format!("{} {} * * *", mm, hh)),
        _ => None,
    }
}

pub fn derive_execution_period(folder_name: &str) -> &'static str {
    let name = folder_name.to_uppercase();
    if name.contains("_MONTHLY") || name.contains("_MTH") {
        "m"
    } else if name.contains("_WEEKLY") || name.contains("_WKL") {
        "w"
    } else if name.contains("_YEARLY") || name.contains("_ANNUAL") {
        "y"
    } else {
        "d"
    }
}
