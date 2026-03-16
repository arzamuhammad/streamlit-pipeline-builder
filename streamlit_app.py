import streamlit as st
import json
import time

st.set_page_config(
    page_title="Jobs & Pipelines",
    page_icon=":material/account_tree:",
    layout="wide",
)

if "pipelines" not in st.session_state:
    st.session_state.pipelines = {}
if "current_pipeline" not in st.session_state:
    st.session_state.current_pipeline = None
if "editing_task" not in st.session_state:
    st.session_state.editing_task = None
if "show_create_pipeline" not in st.session_state:
    st.session_state.show_create_pipeline = False


def get_conn():
    return st.connection("snowflake")


def run_sql(sql):
    conn = get_conn()
    return conn.session().sql(sql).collect()


def get_warehouses():
    try:
        rows = run_sql("SHOW WAREHOUSES")
        return [row["name"] for row in rows]
    except Exception:
        return ["COMPUTE_WH", "XS_WH", "S_WH", "M_WH", "L_WH"]


def get_databases():
    try:
        rows = run_sql("SHOW DATABASES")
        return [row["name"] for row in rows]
    except Exception:
        return []


def get_schemas(db):
    try:
        rows = run_sql(f"SHOW SCHEMAS IN DATABASE {db}")
        return [row["name"] for row in rows]
    except Exception:
        return []


def generate_create_task_sql(task, pipeline_name, all_tasks):
    db = st.session_state.pipelines[pipeline_name].get("database", "")
    schema = st.session_state.pipelines[pipeline_name].get("schema", "")
    prefix = f"{db}.{schema}." if db and schema else ""
    name = task["name"]
    task_type = task["type"]
    warehouse = task.get("warehouse", "")
    schedule = st.session_state.pipelines[pipeline_name].get("schedule", "")
    depends_on = task.get("depends_on", [])
    retries = task.get("retries", 0)
    retry_timeout = task.get("retry_timeout", 60)
    run_if = task.get("run_if", "All succeeded")
    error_integration = task.get("error_integration", "")
    success_integration = task.get("success_integration", "")
    comment = task.get("comment", "")
    suspend_after = task.get("suspend_after_failures", 0)

    quoted_name = f'"{name}"' if " " in name else name
    parts = [f"CREATE OR REPLACE TASK {prefix}{quoted_name}"]

    if warehouse:
        parts.append(f"  WAREHOUSE = {warehouse}")
    else:
        parts.append("  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'")

    if not depends_on and schedule:
        parts.append(f"  SCHEDULE = '{schedule}'")

    if depends_on:
        dep_names = [f'{prefix}"{d}"' if " " in d else f"{prefix}{d}" for d in depends_on]
        parts.append(f"  AFTER {', '.join(dep_names)}")

    if retries and int(retries) > 0:
        parts.append(f"  TASK_AUTO_RETRY_ATTEMPTS = {retries}")

    if suspend_after and int(suspend_after) > 0:
        parts.append(f"  SUSPEND_TASK_AFTER_NUM_FAILURES = {suspend_after}")

    if error_integration:
        parts.append(f"  ERROR_INTEGRATION = {error_integration}")

    if success_integration:
        parts.append(f"  SUCCESS_INTEGRATION = {success_integration}")

    if comment:
        parts.append(f"  COMMENT = '{comment}'")

    if run_if == "At least one succeeded" and depends_on:
        pass

    sql_body = ""
    if task_type == "SQL query":
        sql_body = task.get("sql_query", "SELECT 1")
    elif task_type == "SQL file":
        stage_path = task.get("sql_file_path", "")
        sql_body = f"EXECUTE IMMEDIATE FROM '{stage_path}'"
    elif task_type == "Stored procedure":
        proc = task.get("procedure_name", "")
        sql_body = f"CALL {proc}()"
    elif task_type == "Python script":
        sql_body = task.get("python_body", "SELECT 1")
    elif task_type == "Notebook":
        notebook_path = task.get("notebook_path", "")
        sql_body = f"EXECUTE NOTEBOOK '{notebook_path}'"

    parts.append("AS")
    parts.append(f"  {sql_body};")

    return "\n".join(parts)


def generate_pipeline_sql(pipeline_name):
    pipeline = st.session_state.pipelines[pipeline_name]
    tasks = pipeline.get("tasks", {})
    if not tasks:
        return ""

    sorted_tasks = topological_sort(tasks)
    sqls = []
    for task_name in sorted_tasks:
        task = tasks[task_name]
        sqls.append(generate_create_task_sql(task, pipeline_name, tasks))

    db = pipeline.get("database", "")
    schema = pipeline.get("schema", "")
    prefix = f"{db}.{schema}." if db and schema else ""
    root_tasks = [t for t in tasks.values() if not t.get("depends_on")]
    for rt in root_tasks:
        rt_name = f'"{rt["name"]}"' if " " in rt["name"] else rt["name"]
        sqls.append(f"ALTER TASK {prefix}{rt_name} RESUME;")

    return "\n\n".join(sqls)


def topological_sort(tasks):
    visited = set()
    order = []

    def visit(name):
        if name in visited:
            return
        visited.add(name)
        if name in tasks:
            for dep in tasks[name].get("depends_on", []):
                visit(dep)
            order.append(name)

    for name in tasks:
        visit(name)
    return order


def render_dag(tasks):
    if not tasks:
        st.info("No tasks yet. Click **Create task** to add one.", icon=":material/info:")
        return

    levels = {}
    for name, task in tasks.items():
        deps = task.get("depends_on", [])
        if not deps:
            levels[name] = 0
        else:
            max_dep = 0
            for d in deps:
                if d in levels:
                    max_dep = max(max_dep, levels[d] + 1)
                else:
                    max_dep = 1
            levels[name] = max_dep

    sorted_tasks = topological_sort(tasks)
    for name in sorted_tasks:
        if name not in levels:
            deps = tasks[name].get("depends_on", [])
            if not deps:
                levels[name] = 0
            else:
                levels[name] = max((levels.get(d, 0) for d in deps), default=0) + 1

    max_level = max(levels.values()) if levels else 0
    by_level = {}
    for name, lvl in levels.items():
        by_level.setdefault(lvl, []).append(name)

    for lvl in range(max_level + 1):
        names_at_level = by_level.get(lvl, [])
        if lvl > 0:
            st.caption(":material/arrow_downward:")
        cols = st.columns(max(len(names_at_level), 1))
        for i, name in enumerate(names_at_level):
            task = tasks[name]
            with cols[i]:
                type_icons = {
                    "SQL query": ":material/code:",
                    "SQL file": ":material/description:",
                    "Stored procedure": ":material/functions:",
                    "Python script": ":material/terminal:",
                    "Notebook": ":material/book:",
                }
                icon = type_icons.get(task["type"], ":material/task:")
                deps = task.get("depends_on", [])
                dep_text = f"After: {', '.join(deps)}" if deps else "Root task"

                with st.container(border=True):
                    c1, c2 = st.columns([5, 1])
                    with c1:
                        st.markdown(f"**{icon} {name}**")
                        st.caption(f"{task['type']} · {dep_text}")
                    with c2:
                        if st.button(":material/edit:", key=f"edit_{name}", help="Edit task"):
                            st.session_state.editing_task = name
                            st.rerun()
                        if st.button(":material/delete:", key=f"del_{name}", help="Delete task"):
                            del st.session_state.pipelines[st.session_state.current_pipeline]["tasks"][name]
                            for t in st.session_state.pipelines[st.session_state.current_pipeline]["tasks"].values():
                                if name in t.get("depends_on", []):
                                    t["depends_on"].remove(name)
                            st.rerun()
                        pipeline_data = st.session_state.pipelines[st.session_state.current_pipeline]
                        db = pipeline_data.get("database", "")
                        schema = pipeline_data.get("schema", "")
                        prefix = f"{db}.{schema}." if db and schema else ""
                        quoted = f'"{name}"' if " " in name else name
                        fqn = f"{prefix}{quoted}"
                        if st.button(":material/play_arrow:", key=f"resume_{name}", help="Resume task"):
                            try:
                                run_sql(f"ALTER TASK {fqn} RESUME")
                                st.toast(f"Task **{name}** resumed.", icon=":material/check_circle:")
                            except Exception as e:
                                st.toast(f"Failed to resume: {e}", icon=":material/error:")
                        if st.button(":material/pause:", key=f"suspend_{name}", help="Suspend task"):
                            try:
                                run_sql(f"ALTER TASK {fqn} SUSPEND")
                                st.toast(f"Task **{name}** suspended.", icon=":material/check_circle:")
                            except Exception as e:
                                st.toast(f"Failed to suspend: {e}", icon=":material/error:")


@st.dialog("Create task", width="large")
def create_task_dialog(pipeline_name, editing=None):
    pipeline = st.session_state.pipelines[pipeline_name]
    existing = pipeline.get("tasks", {})
    task_data = existing.get(editing, {}) if editing else {}

    name = st.text_input("Task name", value=task_data.get("name", ""), placeholder="e.g. load_raw_data")
    if editing:
        name = editing

    task_types = ["SQL query", "SQL file", "Stored procedure", "Python script", "Notebook"]
    default_idx = task_types.index(task_data["type"]) if task_data.get("type") in task_types else 0
    task_type = st.segmented_control("Task type", task_types, default=task_types[default_idx], key=f"type_sel_{editing or 'new'}")

    st.space("small")

    warehouses = get_warehouses()
    wh_options = ["Serverless (managed)"] + warehouses
    default_wh = 0
    if task_data.get("warehouse") in warehouses:
        default_wh = warehouses.index(task_data["warehouse"]) + 1
    warehouse = st.selectbox("Warehouse", wh_options, index=default_wh, key=f"wh_{editing or 'new'}")
    if warehouse == "Serverless (managed)":
        warehouse = ""

    if task_type == "SQL query":
        sql_query = st.text_area("SQL query", value=task_data.get("sql_query", ""), height=150, placeholder="SELECT * FROM my_table", key=f"sql_{editing or 'new'}")
    elif task_type == "SQL file":
        sql_file_path = st.text_input("Stage path to SQL file", value=task_data.get("sql_file_path", ""), placeholder="@my_stage/path/to/file.sql", key=f"sqlf_{editing or 'new'}")
    elif task_type == "Stored procedure":
        procedure_name = st.text_input("Procedure name", value=task_data.get("procedure_name", ""), placeholder="my_schema.my_procedure", key=f"proc_{editing or 'new'}")
    elif task_type == "Python script":
        python_body = st.text_area("Python / SQL body", value=task_data.get("python_body", ""), height=150, placeholder="CALL my_python_sproc()", key=f"py_{editing or 'new'}")
    elif task_type == "Notebook":
        notebook_path = st.text_input("Notebook path", value=task_data.get("notebook_path", ""), placeholder="my_db.my_schema.my_notebook", key=f"nb_{editing or 'new'}")

    other_tasks = [t for t in existing if t != editing]
    with st.expander("Dependencies", icon=":material/account_tree:"):
        depends_on = st.multiselect("Depends on", other_tasks, default=task_data.get("depends_on", []), key=f"deps_{editing or 'new'}")
        run_if_options = ["All succeeded", "At least one succeeded"]
        run_if = st.radio("Run if dependencies", run_if_options, index=run_if_options.index(task_data.get("run_if", "All succeeded")), key=f"runif_{editing or 'new'}")

    with st.expander("Retries & error handling", icon=":material/refresh:"):
        retries = st.number_input("Auto retry attempts", min_value=0, max_value=10, value=task_data.get("retries", 0), key=f"ret_{editing or 'new'}")
        suspend_after = st.number_input("Suspend after N failures", min_value=0, max_value=100, value=task_data.get("suspend_after_failures", 0), key=f"sus_{editing or 'new'}")

    with st.expander("Notifications", icon=":material/notifications:"):
        error_integration = st.text_input("Error notification integration", value=task_data.get("error_integration", ""), placeholder="my_error_notification", key=f"errint_{editing or 'new'}")
        success_integration = st.text_input("Success notification integration", value=task_data.get("success_integration", ""), placeholder="my_success_notification", key=f"sucint_{editing or 'new'}")

    with st.expander("Advanced", icon=":material/settings:"):
        comment = st.text_input("Comment", value=task_data.get("comment", ""), key=f"cmt_{editing or 'new'}")

    st.space("small")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Save task", type="primary", use_container_width=True, icon=":material/save:"):
            if not name:
                st.error("Task name is required.")
                return

            task = {
                "name": name,
                "type": task_type,
                "warehouse": warehouse,
                "depends_on": depends_on,
                "run_if": run_if,
                "retries": retries,
                "suspend_after_failures": suspend_after,
                "error_integration": error_integration,
                "success_integration": success_integration,
                "comment": comment,
            }
            if task_type == "SQL query":
                task["sql_query"] = sql_query
            elif task_type == "SQL file":
                task["sql_file_path"] = sql_file_path
            elif task_type == "Stored procedure":
                task["procedure_name"] = procedure_name
            elif task_type == "Python script":
                task["python_body"] = python_body
            elif task_type == "Notebook":
                task["notebook_path"] = notebook_path

            st.session_state.pipelines[pipeline_name]["tasks"][name] = task
            st.session_state.editing_task = None
            st.rerun()
    with c2:
        if st.button("Cancel", use_container_width=True):
            st.session_state.editing_task = None
            st.rerun()


@st.dialog("Create pipeline", width="large")
def create_pipeline_dialog():
    name = st.text_input("Pipeline name", placeholder="e.g. daily_etl_pipeline")

    c1, c2 = st.columns(2)
    with c1:
        databases = get_databases()
        db = st.selectbox("Database", databases if databases else [""], key="new_pipe_db")
    with c2:
        schemas = get_schemas(db) if db else []
        schema = st.selectbox("Schema", schemas if schemas else [""], key="new_pipe_schema")

    schedule_type = st.radio("Schedule type", ["Cron", "Interval", "None (manual)"], horizontal=True)
    schedule = ""
    if schedule_type == "Cron":
        cron_presets = {
            "Every hour": "USING CRON 0 * * * * UTC",
            "Every day at midnight": "USING CRON 0 0 * * * UTC",
            "Every day at 6 AM": "USING CRON 0 6 * * * UTC",
            "Weekdays at 8 AM": "USING CRON 0 8 * * 1-5 UTC",
            "Custom": "",
        }
        preset = st.selectbox("Preset", list(cron_presets.keys()))
        if preset == "Custom":
            schedule = st.text_input("Cron expression", placeholder="USING CRON 0 * * * * UTC")
        else:
            schedule = cron_presets[preset]
            st.code(schedule, language=None)
    elif schedule_type == "Interval":
        c1, c2 = st.columns(2)
        with c1:
            interval_val = st.number_input("Every", min_value=1, value=60)
        with c2:
            interval_unit = st.selectbox("Unit", ["MINUTES", "HOURS", "SECONDS"])
        schedule = f"{interval_val} {interval_unit}"

    comment = st.text_input("Description", placeholder="Optional description of this pipeline")

    st.space("small")
    if st.button("Create pipeline", type="primary", use_container_width=True, icon=":material/add:"):
        if not name:
            st.error("Pipeline name is required.")
            return
        if name in st.session_state.pipelines:
            st.error("A pipeline with this name already exists.")
            return

        st.session_state.pipelines[name] = {
            "database": db,
            "schema": schema,
            "schedule": schedule,
            "comment": comment,
            "tasks": {},
        }
        st.session_state.current_pipeline = name
        st.session_state.show_create_pipeline = False
        st.rerun()


@st.dialog("Review & deploy", width="large")
def deploy_dialog(pipeline_name):
    sql = generate_pipeline_sql(pipeline_name)
    st.markdown("Review the generated SQL before deploying:")
    st.code(sql, language="sql", line_numbers=True)

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Deploy to Snowflake", type="primary", use_container_width=True, icon=":material/rocket_launch:"):
            with st.spinner("Deploying tasks..."):
                try:
                    statements = [s.strip() for s in sql.split(";") if s.strip()]
                    for stmt in statements:
                        run_sql(stmt)
                    st.success("Pipeline deployed successfully!", icon=":material/check_circle:")
                    st.balloons()
                except Exception as e:
                    st.error(f"Deployment failed: {e}")
    with c2:
        if st.button("Cancel", use_container_width=True):
            st.rerun()


@st.dialog("Confirm delete", width="small")
def confirm_delete_dialog(pipeline_name):
    st.markdown(f"Delete pipeline **{pipeline_name}**? This cannot be undone.")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Delete", type="primary", use_container_width=True):
            del st.session_state.pipelines[pipeline_name]
            if st.session_state.current_pipeline == pipeline_name:
                st.session_state.current_pipeline = None
            st.rerun()
    with c2:
        if st.button("Cancel", use_container_width=True, key="cancel_del"):
            st.rerun()


with st.sidebar:
    st.title(":material/account_tree: Pipelines")

    if st.button("New pipeline", use_container_width=True, type="primary", icon=":material/add:"):
        st.session_state.show_create_pipeline = True

    st.space("small")

    if st.session_state.pipelines:
        for pname in st.session_state.pipelines:
            pipeline = st.session_state.pipelines[pname]
            task_count = len(pipeline.get("tasks", {}))
            is_selected = st.session_state.current_pipeline == pname
            label = f"{'**' if is_selected else ''}{pname}{'**' if is_selected else ''}"
            with st.container(border=is_selected):
                c1, c2 = st.columns([5, 1])
                with c1:
                    if st.button(f":material/account_tree: {pname}", key=f"sel_{pname}", use_container_width=True, type="tertiary"):
                        st.session_state.current_pipeline = pname
                        st.rerun()
                    st.caption(f"{task_count} task{'s' if task_count != 1 else ''} · {pipeline.get('schedule', 'No schedule')}")
                with c2:
                    if st.button(":material/delete:", key=f"delpipe_{pname}", help="Delete pipeline"):
                        confirm_delete_dialog(pname)
    else:
        st.caption("No pipelines yet")

if st.session_state.show_create_pipeline:
    create_pipeline_dialog()

if st.session_state.editing_task and st.session_state.current_pipeline:
    create_task_dialog(st.session_state.current_pipeline, editing=st.session_state.editing_task)

cp = st.session_state.current_pipeline
if cp and cp in st.session_state.pipelines:
    pipeline = st.session_state.pipelines[cp]

    h1, h2 = st.columns([4, 2])
    with h1:
        st.title(f":material/account_tree: {cp}")
    with h2:
        bc1, bc2, bc3 = st.columns(3)
        with bc1:
            if st.button("Create task", type="primary", icon=":material/add:", use_container_width=True):
                create_task_dialog(cp)
        with bc2:
            if st.button("Deploy", icon=":material/rocket_launch:", use_container_width=True):
                if pipeline.get("tasks"):
                    deploy_dialog(cp)
                else:
                    st.toast("Add at least one task before deploying.", icon=":material/warning:")
        with bc3:
            if st.button("View SQL", icon=":material/code:", use_container_width=True):
                st.session_state[f"show_sql_{cp}"] = not st.session_state.get(f"show_sql_{cp}", False)
                st.rerun()

    info_cols = st.columns(4)
    with info_cols[0]:
        st.metric("Tasks", len(pipeline.get("tasks", {})))
    with info_cols[1]:
        st.metric("Database", pipeline.get("database", "—"))
    with info_cols[2]:
        st.metric("Schema", pipeline.get("schema", "—"))
    with info_cols[3]:
        sched = pipeline.get("schedule", "")
        st.metric("Schedule", sched if sched else "Manual")

    if st.session_state.get(f"show_sql_{cp}"):
        sql = generate_pipeline_sql(cp)
        if sql:
            st.code(sql, language="sql", line_numbers=True)
        else:
            st.caption("No tasks to generate SQL for.")

    st.space("small")
    st.subheader("Task graph")
    render_dag(pipeline.get("tasks", {}))

else:
    st.title(":material/account_tree: Jobs & Pipelines")
    st.markdown("Build and manage Snowflake data pipelines visually.")

    st.space("large")

    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            st.markdown(":material/add_circle: **Create pipeline**")
            st.caption("Define a new data pipeline with tasks, schedules, and dependencies.")
            if st.button("Get started", key="gs1", icon=":material/arrow_forward:"):
                st.session_state.show_create_pipeline = True
                st.rerun()
    with c2:
        with st.container(border=True):
            st.markdown(":material/account_tree: **Visual DAG builder**")
            st.caption("Drag-and-drop style task creation with dependency visualization.")
    with c3:
        with st.container(border=True):
            st.markdown(":material/rocket_launch: **One-click deploy**")
            st.caption("Generate and execute CREATE TASK SQL directly in Snowflake.")

    if st.session_state.pipelines:
        st.space("large")
        st.subheader("Your pipelines")
        for pname, pdata in st.session_state.pipelines.items():
            task_count = len(pdata.get("tasks", {}))
            with st.container(border=True):
                c1, c2 = st.columns([5, 1])
                with c1:
                    st.markdown(f"**:material/account_tree: {pname}**")
                    st.caption(f"{task_count} tasks · {pdata.get('database', '')}.{pdata.get('schema', '')} · {pdata.get('schedule', 'Manual')}")
                with c2:
                    if st.button("Open", key=f"open_{pname}", type="primary"):
                        st.session_state.current_pipeline = pname
                        st.rerun()
