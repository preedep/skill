use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Folder {
    pub datacenter: String,
    pub folder_name: String,
    pub folder_order_method: String,
    pub platform: String,
    pub dayscal: String,
    pub confcal: String,
    pub jobs: Vec<Job>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Job {
    pub jobname: String,
    pub jobisn: String,
    pub parent_folder: String,
    pub application: String,
    pub sub_application: String,
    pub nodeid: String,
    pub run_as: String,
    pub appl_type: String,
    pub appl_form: String,
    pub cmdline: String,
    pub memname: String,
    pub timefrom: String,
    pub timeto: String,
    pub days: String,
    pub weekdays: String,
    pub cyclic: String,
    pub interval: String,
    pub dayscal: String,
    pub confcal: String,
    pub priority: String,
    pub critical: String,
    pub tasktype: String,
    pub description: String,
    pub jan: String,
    pub feb: String,
    pub mar: String,
    pub apr: String,
    pub may: String,
    pub jun: String,
    pub jul: String,
    pub aug: String,
    pub sep: String,
    pub oct: String,
    pub nov: String,
    pub dec: String,
    pub inconds: Vec<InCond>,
    pub outconds: Vec<OutCond>,
    pub variables: Vec<Variable>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InCond {
    pub name: String,
    pub odate: String,
    pub and_or: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutCond {
    pub name: String,
    pub odate: String,
    pub sign: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Variable {
    pub name: String,
    pub value: String,
}
