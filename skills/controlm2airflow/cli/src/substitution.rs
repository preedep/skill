use regex::Regex;

pub fn translate(value: &str) -> String {
    let mut result = value.to_string();

    // %%SUBSTR %%PREV patterns (must be before %%PREV)
    let substr_prev_re = Regex::new(r"%%SUBSTR\s+%%PREV\s+(\d+)\s+(\d+)").unwrap();
    result = substr_prev_re
        .replace_all(&result, |caps: &regex::Captures| {
            let start: usize = caps[1].parse().unwrap_or(1);
            let _len: usize = caps[2].parse().unwrap_or(1);
            match start {
                1 => "{{ (logical_date - macros.timedelta(days=1)).strftime('%Y') }}".to_string(),
                5 => "{{ (logical_date - macros.timedelta(days=1)).strftime('%m') }}".to_string(),
                7 => "{{ (logical_date - macros.timedelta(days=1)).strftime('%d') }}".to_string(),
                _ => format!(
                    "{{{{ (logical_date - macros.timedelta(days=1)).strftime('%Y%m%d') }}}}[{}]  # TODO: verify SUBSTR offset",
                    start
                ),
            }
        })
        .to_string();

    // %%PREV
    result = result.replace(
        "%%PREV",
        "{{ (logical_date - macros.timedelta(days=1)).strftime('%Y%m%d') }}",
    );

    // ODATE variants
    result = result.replace("%%$ODATE", "{{ ds_nodash }}");
    result = result.replace("%%ODATE", "{{ ds_nodash }}");

    // Date parts
    result = result.replace("%%$YEAR.", "{{ logical_date.strftime('%Y') }}");
    result = result.replace("%%YEAR.", "{{ logical_date.strftime('%Y') }}");
    result = result.replace("%%MONTH.", "{{ logical_date.strftime('%m') }}");
    result = result.replace("%%DAY.", "{{ logical_date.strftime('%d') }}");

    // HTML entities
    result = result.replace("&quot;", "\"");
    result = result.replace("&amp;", "&");
    result = result.replace("&lt;", "<");
    result = result.replace("&gt;", ">");
    result = result.replace("%4E", "\\n");

    // Remaining %% expressions — keep as literal placeholder (safe inside strings)
    let remaining_re = Regex::new(r"%%\w+").unwrap();
    result = remaining_re
        .replace_all(&result, |caps: &regex::Captures| {
            format!("CTRLM_{}", &caps[0].trim_start_matches('%'))
        })
        .to_string();

    result
}
