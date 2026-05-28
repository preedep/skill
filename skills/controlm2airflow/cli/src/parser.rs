use anyhow::{Context, Result};
use roxmltree::Document;

use crate::model::{Folder, InCond, Job, OutCond, Variable};

pub fn parse(xml: &str) -> Result<Vec<Folder>> {
    let doc = Document::parse(xml).context("Failed to parse XML")?;
    let root = doc.root_element();

    let mut folders = Vec::new();

    let root_tag = root.tag_name().name();
    if matches!(root_tag, "FOLDER" | "SCHED_TABLE" | "TABLE" | "SMART_FOLDER" | "SMART_TABLE") {
        folders.push(parse_folder(&root));
    } else {
        for node in root.children() {
            if !node.is_element() {
                continue;
            }
            let tag = node.tag_name().name();
            if matches!(tag, "FOLDER" | "SCHED_TABLE" | "TABLE" | "SMART_FOLDER" | "SMART_TABLE") {
                folders.push(parse_folder(&node));
            }
        }
    }

    Ok(folders)
}

fn parse_folder(node: &roxmltree::Node) -> Folder {
    let attr = |name: &str| node.attribute(name).unwrap_or("").to_string();

    let mut jobs = Vec::new();
    for child in node.children() {
        if child.is_element() && child.tag_name().name() == "JOB" {
            jobs.push(parse_job(&child));
        }
    }

    Folder {
        datacenter: attr("DATACENTER"),
        folder_name: attr("FOLDER_NAME"),
        folder_order_method: attr("FOLDER_ORDER_METHOD"),
        platform: attr("PLATFORM"),
        dayscal: attr("DAYSCAL"),
        confcal: attr("CONFCAL"),
        jobs,
    }
}

fn parse_job(node: &roxmltree::Node) -> Job {
    let attr = |name: &str| node.attribute(name).unwrap_or("").to_string();

    let mut inconds = Vec::new();
    let mut outconds = Vec::new();
    let mut variables = Vec::new();

    for child in node.children() {
        if !child.is_element() {
            continue;
        }
        match child.tag_name().name() {
            "INCOND" => inconds.push(InCond {
                name: child.attribute("NAME").unwrap_or("").to_string(),
                odate: child.attribute("ODATE").unwrap_or("").to_string(),
                and_or: child.attribute("AND_OR").unwrap_or("A").to_string(),
            }),
            "OUTCOND" => outconds.push(OutCond {
                name: child.attribute("NAME").unwrap_or("").to_string(),
                odate: child.attribute("ODATE").unwrap_or("").to_string(),
                sign: child.attribute("SIGN").unwrap_or("+").to_string(),
            }),
            "VARIABLE" | "AUTOEDIT2" => variables.push(Variable {
                name: child.attribute("NAME").unwrap_or("").to_string(),
                value: child.attribute("VALUE").unwrap_or("").to_string(),
            }),
            _ => {}
        }
    }

    Job {
        jobname: attr("JOBNAME"),
        jobisn: attr("JOBISN"),
        parent_folder: attr("PARENT_FOLDER"),
        application: attr("APPLICATION"),
        sub_application: attr("SUB_APPLICATION"),
        nodeid: attr("NODEID"),
        run_as: attr("RUN_AS"),
        appl_type: attr("APPL_TYPE"),
        appl_form: attr("APPL_FORM"),
        cmdline: attr("CMDLINE"),
        memname: attr("MEMNAME"),
        timefrom: attr("TIMEFROM"),
        timeto: attr("TIMETO"),
        days: attr("DAYS"),
        weekdays: attr("WEEKDAYS"),
        cyclic: attr("CYCLIC"),
        interval: attr("INTERVAL"),
        dayscal: attr("DAYSCAL"),
        confcal: attr("CONFCAL"),
        priority: attr("PRIORITY"),
        critical: attr("CRITICAL"),
        tasktype: attr("TASKTYPE"),
        description: attr("DESCRIPTION"),
        jan: attr("JAN"),
        feb: attr("FEB"),
        mar: attr("MAR"),
        apr: attr("APR"),
        may: attr("MAY"),
        jun: attr("JUN"),
        jul: attr("JUL"),
        aug: attr("AUG"),
        sep: attr("SEP"),
        oct: attr("OCT"),
        nov: attr("NOV"),
        dec: attr("DEC"),
        inconds,
        outconds,
        variables,
    }
}
