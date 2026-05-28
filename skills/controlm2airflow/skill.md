Skill: Convert Control-M jobs to Apache Airflow DAGs

## Purpose
This skill converts Control-M jobs (XML) to Apache Airflow DAGs. (DAGs python code)

## Input
The input is a Control-M job XML file.

## Output
The output is a Python script that defines an Apache Airflow DAG. must follow standard patterns


| Pattern | Description |Example|
|---------|-------------|------|
| Naming convention DAG file | DAG file follow  `<company>-<app_id>-<app_code>-<folder_name>-<env>.py` for dag_name is existing folder name of job control  use small case (eg. acme-AP1234-pyment-abc-monthly-tab-dev.py) | `acme-AP1234-payment-abc-monthly-tab-dev.py` |
| Naming convention DAG ID | same as filename without `.py` | `acme-AP1234-payment-abc-monthly-tab` |
| Naming convention (task) | Task IDs follow `<app_id>-<app_code>-task_<task_name>-<execution_period>` for task_name is existing job name of job control  use small case (eg. AP1234-pyment-task_rt-rb2cm005-d) , for execution_period use small case (eg.  d=daily, w=weekly, m=monthly,y=yearly) | `AP1234-pyment-task_rt-rb2cm005-d` |
| Control-M Folder | Smart Folder / Control-M Folder replacement with DAG level |   |
| Control-M Job | Control-M Job replacement with DAG task |  |
| Control-M Job Type | Control-M Job Type replacement with Operator or Sensor |  |
| Control-M Job dependencies | >> operator is used to define task dependencies | `task_1 >> task_2` |
| ODATE Replacement | ODATE values are replaced with Airflow 'logical date' | `{{ ds }}` |
| Error alert | uses email_on_failure attriute which default is false (add all DAGs) | `email_on_failure=False`|
| Retry alert | uses email_on_retry attriute which default is false (add all DAGs) | `email_on_retry=False` |


## Behavior
1. Parse the input XML and extract all Control-M folder and job metadata.
2. Group jobs by folder — each folder produces one DAG file.
3. For each folder:
   a. Derive DAG file name and DAG ID from `<company>-<app_id>-<app_code>-<folder_name>-<env>` (lowercased).
   b. Map the folder's schedule to an Airflow `schedule` parameter.
   c. For each job in the folder, create one Airflow task:
      - Derive task ID from `<app_id>-<app_code>-task_<job_name>-<period>` (lowercased).
      - Select the operator based on job type (see Constraints).
      - Apply SLA if defined on the job.
      - Wire `on_failure_callback` to the standard alert hook.
   d. Build task dependencies from Control-M job dependencies (`INCOND`/`OUTCOND`).
   e. Add `ExternalTaskSensor` for any dependency referencing a job outside this folder.
4. Write the generated DAG to a `.py` file.
5. For any unsupported job type, emit a `# TODO:` comment at the task location and log a warning.


## Constraints & Assumptions
- One Control-M folder = one DAG file
- Unsupported job types emit a `# TODO:` comment in the output and log a warning
- All identifiers lowercased
- Target: Airflow 3.x with classic operators (no Taskflow API)

## Reference
- [`controlm-schema.xsd`](controlm-schema.xsd) — official Control-M XML schema (DEFTABLE, FOLDER, SMART_FOLDER, JOB, INCOND, OUTCOND, VARIABLE, etc.)

> **When to consult the schema:** Only read this file when the input XML contains an unfamiliar element or attribute, when validating which child elements are valid inside a given container (e.g. `SMART_FOLDER` vs `FOLDER` vs `SUB_FOLDER`), or when resolving an edge case not covered by the Behavior or Constraints sections above. For standard `FOLDER`/`JOB` inputs, the schema is not needed.
