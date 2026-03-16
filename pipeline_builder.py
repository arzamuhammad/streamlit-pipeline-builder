import streamlit as st
import json
import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional
from enum import Enum

st.set_page_config(page_title="Snowflake Pipeline Builder", layout="wide", initial_sidebar_state="collapsed")

class TaskType(str, Enum):
    SQL = "SQL"
    PYTHON = "Python"
    NOTEBOOK = "Notebook"

class SQLTaskType(str, Enum):
    QUERY = "Query"
    FILE = "File"
    ALERT = "Alert"

class DependencyCondition(str, Enum):
    ALL_SUCCEEDED = "All succeeded"
    AT_LEAST_ONE_SUCCEEDED = "At least one succeeded"
    NONE_FAILED = "None failed"
    ALL_DONE = "All done"

@dataclass
class Task:
    id: str
    name: str
    task_type: str = "SQL"
    sql_task_type: str = "Query"
    source: str = "Workspace"
    path: str = ""
    sql_query: str = ""
    warehouse: str = ""
    depends_on: list = field(default_factory=list)
    dependency_condition: str = "All succeeded"
    parameters: dict = field(default_factory=dict)
    notifications: list = field(default_factory=list)
    retry_count: int = 0
    retry_delay_seconds: int = 60
    metric_thresholds: list = field(default_factory=list)

if "tasks" not in st.session_state:
    st.session_state.tasks = {}
if "selected_task_id" not in st.session_state:
    st.session_state.selected_task_id = None
if "show_add_task_modal" not in st.session_state:
    st.session_state.show_add_task_modal = False
if "show_ddl_modal" not in st.session_state:
    st.session_state.show_ddl_modal = False
if "pipeline_name" not in st.session_state:
    st.session_state.pipeline_name = "my_pipeline"

st.markdown("""
<style>
.task-card {
    background: white;
    border: 2px solid #e0e0e0;
    border-radius: 8px;
    padding: 12px;
    margin: 8px;
    min-width: 200px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}
.task-card.selected {
    border-color: #0068C9;
    background: #f0f7ff;
}
.task-card-header {
    font-weight: bold;
    font-size: 14px;
    margin-bottom: 4px;
}
.task-card-info {
    font-size: 12px;
    color: #666;
}
.pipeline-canvas {
    background: #f8f9fa;
    border-radius: 8px;
    min-height: 300px;
    padding: 20px;
    border: 1px dashed #ccc;
}
.add-task-btn {
    background: #0068C9;
    color: white;
    border: none;
    padding: 8px 16px;
    border-radius: 4px;
    cursor: pointer;
}
.empty-state {
    text-align: center;
    padding: 60px;
    color: #666;
}
.modal-option {
    padding: 12px;
    border: 1px solid #e0e0e0;
    border-radius: 8px;
    margin: 8px 0;
    cursor: pointer;
}
.modal-option:hover {
    background: #f5f5f5;
}
div[data-testid="stForm"] {
    background: white;
    padding: 20px;
    border-radius: 8px;
}
</style>
""", unsafe_allow_html=True)

def generate_snowflake_ddl():
    if not st.session_state.tasks:
        return "-- No tasks defined"
    
    ddl_statements = []
    pipeline_name = st.session_state.pipeline_name
    
    sorted_tasks = []
    remaining = list(st.session_state.tasks.values())
    added_ids = set()
    
    while remaining:
        for task in remaining[:]:
            deps_satisfied = all(d in added_ids for d in task.depends_on)
            if deps_satisfied:
                sorted_tasks.append(task)
                added_ids.add(task.id)
                remaining.remove(task)
    
    for task in sorted_tasks:
        task_name = f"{pipeline_name}_{task.name}".upper().replace(" ", "_")
        
        if task.task_type == "SQL":
            if task.sql_task_type == "Query":
                sql_body = task.sql_query if task.sql_query else "SELECT 1"
            elif task.sql_task_type == "File":
                sql_body = f"EXECUTE IMMEDIATE FROM '@stage/{task.path}'" if task.path else "SELECT 1"
            else:
                sql_body = f"CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(...)" if task.sql_task_type == "Alert" else "SELECT 1"
        elif task.task_type == "Python":
            sql_body = f"CALL {task.path}()" if task.path else "SELECT 1"
        elif task.task_type == "Notebook":
            sql_body = f"EXECUTE NOTEBOOK \"{task.path}\"" if task.path else "SELECT 1"
        else:
            sql_body = "SELECT 1"
        
        ddl = f"CREATE OR REPLACE TASK {task_name}\n"
        
        if task.warehouse:
            ddl += f"  WAREHOUSE = {task.warehouse}\n"
        
        if not task.depends_on:
            ddl += "  SCHEDULE = 'USING CRON 0 0 * * * UTC'\n"
        else:
            dep_names = []
            for dep_id in task.depends_on:
                if dep_id in st.session_state.tasks:
                    dep_task = st.session_state.tasks[dep_id]
                    dep_names.append(f"{pipeline_name}_{dep_task.name}".upper().replace(" ", "_"))
            if dep_names:
                ddl += f"  AFTER {', '.join(dep_names)}\n"
        
        if task.retry_count > 0:
            ddl += f"  SUSPEND_TASK_AFTER_NUM_FAILURES = {task.retry_count}\n"
        
        ddl += f"AS\n{sql_body};\n"
        ddl_statements.append(ddl)
    
    resume_statements = []
    for task in reversed(sorted_tasks):
        task_name = f"{pipeline_name}_{task.name}".upper().replace(" ", "_")
        resume_statements.append(f"ALTER TASK {task_name} RESUME;")
    
    return "\n".join(ddl_statements) + "\n\n-- Resume tasks (run in reverse dependency order)\n" + "\n".join(resume_statements)

def add_task(task_type: str):
    task_id = str(uuid.uuid4())[:8]
    new_task = Task(
        id=task_id,
        name=f"task_{len(st.session_state.tasks) + 1}",
        task_type=task_type
    )
    st.session_state.tasks[task_id] = new_task
    st.session_state.selected_task_id = task_id
    st.session_state.show_add_task_modal = False

def delete_task(task_id: str):
    if task_id in st.session_state.tasks:
        del st.session_state.tasks[task_id]
        for task in st.session_state.tasks.values():
            if task_id in task.depends_on:
                task.depends_on.remove(task_id)
        if st.session_state.selected_task_id == task_id:
            st.session_state.selected_task_id = None

def render_task_card(task: Task, is_selected: bool):
    card_class = "task-card selected" if is_selected else "task-card"
    
    icon = "📄" if task.task_type == "SQL" else "🐍" if task.task_type == "Python" else "📓"
    
    path_display = task.path[:30] + "..." if len(task.path) > 30 else task.path
    if not path_display:
        path_display = task.sql_query[:30] + "..." if task.sql_query and len(task.sql_query) > 30 else (task.sql_query or "No source specified")
    
    warehouse_display = task.warehouse if task.warehouse else "No warehouse selected"
    
    return f"""
    <div class="{card_class}">
        <div class="task-card-header">{task.name}</div>
        <div class="task-card-info">{icon} {path_display}</div>
        <div class="task-card-info">🏭 {warehouse_display}</div>
    </div>
    """

st.markdown("### ❄️ Snowflake Pipeline Builder")

col_header1, col_header2, col_header3 = st.columns([2, 1, 1])
with col_header1:
    st.session_state.pipeline_name = st.text_input("Pipeline Name", value=st.session_state.pipeline_name, label_visibility="collapsed", placeholder="Pipeline name...")
with col_header3:
    if st.button("📋 Generate DDL", type="primary", use_container_width=True):
        st.session_state.show_ddl_modal = True

st.markdown("---")

tab1, tab2 = st.tabs(["Tasks", "Runs"])

with tab1:
    if st.session_state.show_ddl_modal:
        st.markdown("#### Generated Snowflake Task DDL")
        ddl = generate_snowflake_ddl()
        st.code(ddl, language="sql")
        if st.button("Close"):
            st.session_state.show_ddl_modal = False
        st.markdown("---")
    
    if not st.session_state.tasks:
        st.markdown("""
        <div class="empty-state">
            <h3>Add your first task</h3>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown("##### Quick Add")
            if st.button("📓 Notebook", use_container_width=True, help="Run a notebook"):
                add_task("Notebook")
                st.rerun()
            
            st.markdown("---")
            st.markdown("*or*")
            if st.button("➕ Add another task type", type="primary", use_container_width=True):
                st.session_state.show_add_task_modal = True
                st.rerun()
    else:
        col_canvas, col_config = st.columns([2, 1])
        
        with col_canvas:
            st.markdown('<div class="pipeline-canvas">', unsafe_allow_html=True)
            
            levels = {}
            for task in st.session_state.tasks.values():
                if not task.depends_on:
                    levels.setdefault(0, []).append(task)
                else:
                    max_dep_level = 0
                    for dep_id in task.depends_on:
                        for level, tasks in levels.items():
                            if any(t.id == dep_id for t in tasks):
                                max_dep_level = max(max_dep_level, level + 1)
                    levels.setdefault(max_dep_level, []).append(task)
            
            for level in sorted(levels.keys()):
                tasks_at_level = levels[level]
                cols = st.columns(len(tasks_at_level) + 1)
                
                for i, task in enumerate(tasks_at_level):
                    with cols[i]:
                        is_selected = st.session_state.selected_task_id == task.id
                        
                        btn_type = "primary" if is_selected else "secondary"
                        if st.button(
                            f"**{task.name}**\n\n{'📄' if task.task_type == 'SQL' else '🐍' if task.task_type == 'Python' else '📓'} {task.task_type}",
                            key=f"task_{task.id}",
                            use_container_width=True,
                            type=btn_type
                        ):
                            st.session_state.selected_task_id = task.id
                            st.rerun()
                        
                        if task.depends_on:
                            dep_names = [st.session_state.tasks[d].name for d in task.depends_on if d in st.session_state.tasks]
                            if dep_names:
                                st.caption(f"↳ After: {', '.join(dep_names)}")
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            if st.button("➕ Add task", type="primary"):
                st.session_state.show_add_task_modal = True
                st.rerun()
        
        with col_config:
            if st.session_state.selected_task_id and st.session_state.selected_task_id in st.session_state.tasks:
                task = st.session_state.tasks[st.session_state.selected_task_id]
                
                col_actions = st.columns([1, 1, 1, 1])
                with col_actions[0]:
                    st.button("▶️", help="Run task")
                with col_actions[1]:
                    st.button("📋", help="Duplicate")
                with col_actions[2]:
                    if st.button("🗑️", help="Delete"):
                        delete_task(task.id)
                        st.rerun()
                
                st.markdown("---")
                
                task.name = st.text_input("Task name*", value=task.name, key="task_name")
                
                task.task_type = st.selectbox(
                    "Type*",
                    options=["SQL", "Python", "Notebook"],
                    index=["SQL", "Python", "Notebook"].index(task.task_type),
                    key="task_type"
                )
                
                if task.task_type == "SQL":
                    task.sql_task_type = st.selectbox(
                        "SQL task*",
                        options=["Query", "File", "Alert"],
                        index=["Query", "File", "Alert"].index(task.sql_task_type) if task.sql_task_type in ["Query", "File", "Alert"] else 0,
                        key="sql_task_type"
                    )
                    
                    if task.sql_task_type == "Query":
                        task.sql_query = st.text_area("SQL Query*", value=task.sql_query, height=100, key="sql_query")
                    else:
                        task.source = st.selectbox(
                            "Source*",
                            options=["Workspace", "Git", "Stage"],
                            index=["Workspace", "Git", "Stage"].index(task.source) if task.source in ["Workspace", "Git", "Stage"] else 0,
                            key="source"
                        )
                        task.path = st.text_input("Path*", value=task.path, key="path", placeholder="Select SQL file")
                
                elif task.task_type == "Python":
                    task.path = st.text_input("Stored Procedure*", value=task.path, key="sp_path", placeholder="schema.procedure_name")
                
                elif task.task_type == "Notebook":
                    task.path = st.text_input("Notebook Path*", value=task.path, key="nb_path", placeholder="database.schema.notebook_name")
                
                task.warehouse = st.text_input("SQL warehouse*", value=task.warehouse, key="warehouse", placeholder="Select warehouse...")
                
                other_tasks = {t.id: t.name for t in st.session_state.tasks.values() if t.id != task.id}
                if other_tasks:
                    current_deps = [d for d in task.depends_on if d in other_tasks]
                    selected_deps = st.multiselect(
                        "Depends on",
                        options=list(other_tasks.keys()),
                        default=current_deps,
                        format_func=lambda x: other_tasks[x],
                        key="depends_on"
                    )
                    task.depends_on = selected_deps
                    
                    if selected_deps:
                        task.dependency_condition = st.selectbox(
                            "Run if dependencies",
                            options=["All succeeded", "At least one succeeded", "None failed", "All done"],
                            index=["All succeeded", "At least one succeeded", "None failed", "All done"].index(task.dependency_condition),
                            key="dep_condition"
                        )
                
                with st.expander("Parameters"):
                    st.caption("Add key-value parameters")
                    param_key = st.text_input("Key", key="param_key", label_visibility="collapsed", placeholder="Key")
                    param_val = st.text_input("Value", key="param_val", label_visibility="collapsed", placeholder="Value")
                    if st.button("➕ Add", key="add_param"):
                        if param_key and param_val:
                            task.parameters[param_key] = param_val
                    if task.parameters:
                        for k, v in task.parameters.items():
                            st.text(f"{k}: {v}")
                
                with st.expander("Notifications"):
                    st.caption("Configure email/webhook notifications")
                    notif = st.text_input("Email/Webhook", key="notif_input", placeholder="email@example.com")
                    if st.button("➕ Add", key="add_notif"):
                        if notif:
                            task.notifications.append(notif)
                    for n in task.notifications:
                        st.text(n)
                
                with st.expander("Retries"):
                    task.retry_count = st.number_input("Retry count", min_value=0, max_value=10, value=task.retry_count, key="retry_count")
                    task.retry_delay_seconds = st.number_input("Retry delay (seconds)", min_value=1, max_value=3600, value=task.retry_delay_seconds, key="retry_delay")
                
                with st.expander("Metric thresholds"):
                    st.caption("Add metric thresholds for monitoring")
                    if st.button("➕ Add threshold", key="add_threshold"):
                        task.metric_thresholds.append({"metric": "", "threshold": 0})
            else:
                st.info("Select a task to configure")
    
    if st.session_state.show_add_task_modal:
        st.markdown("---")
        st.markdown("### Add Task")
        
        st.markdown("**Code files**")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📓 Notebook\n\nRun a notebook", use_container_width=True):
                add_task("Notebook")
                st.rerun()
            if st.button("📄 SQL query\n\nRun a SQL query", use_container_width=True):
                add_task("SQL")
                st.rerun()
        with col2:
            if st.button("🐍 Python script\n\nRun a Python file", use_container_width=True):
                add_task("Python")
                st.rerun()
            if st.button("📁 SQL file\n\nRun a SQL file", use_container_width=True):
                task_id = str(uuid.uuid4())[:8]
                new_task = Task(id=task_id, name=f"task_{len(st.session_state.tasks) + 1}", task_type="SQL", sql_task_type="File")
                st.session_state.tasks[task_id] = new_task
                st.session_state.selected_task_id = task_id
                st.session_state.show_add_task_modal = False
                st.rerun()
        
        col3, col4 = st.columns(2)
        with col3:
            if st.button("🔔 SQL alert\n\nEvaluate a SQL alert", use_container_width=True):
                task_id = str(uuid.uuid4())[:8]
                new_task = Task(id=task_id, name=f"task_{len(st.session_state.tasks) + 1}", task_type="SQL", sql_task_type="Alert")
                st.session_state.tasks[task_id] = new_task
                st.session_state.selected_task_id = task_id
                st.session_state.show_add_task_modal = False
                st.rerun()
        
        if st.button("Cancel"):
            st.session_state.show_add_task_modal = False
            st.rerun()

with tab2:
    st.info("Pipeline run history will appear here after tasks are executed in Snowflake")
    st.markdown("""
    | Run ID | Status | Start Time | Duration | Tasks |
    |--------|--------|------------|----------|-------|
    | *No runs yet* | - | - | - | - |
    """)
