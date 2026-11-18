# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Run dbt from Azure DevOps with the executing identity
# Isolated dbt install, shallow clone of one commit from Azure DevOps, SQL endpoint metadata refresh,
# runtime profile with the executing identity's SQL token, `dbt build`, and run artefacts in
# `Files/dbt_runs/<run_id>_<command>/`.
# Notes:
# - Run from a pipeline, the notebook executes as the pipeline's last modifier, not as whoever clicks Run.
# - The identity needs DDL/DML on the Warehouse and SELECT on the Lakehouse SQL endpoint.
# - Development setup only: production runs should use a technical identity (service principal or
#   workspace identity) and read the Azure DevOps credential from Key Vault.

# PARAMETERS CELL ********************

# Parameters (pipelines override these; in the Fabric UI this shows as the "Parameters" cell).
p_repo_url = "https://dev.azure.com/pulze-azure-devops/Oyster/_git/Oyster"
p_branch = "main"
p_dbt_subfolder = "dbt_oyster_path"
p_dbt_target = "dev"
p_dbt_command = "build"
p_dbt_args = ""              # e.g. "--select silver"
p_ado_token = ""             # Azure DevOps PAT (Code: Read) or Entra token; passed at run time, never saved
p_key_vault_url = ""         # placeholder: when set, the PAT is read from Key Vault instead of p_ado_token
p_ado_secret_name = ""
p_sql_endpoint_id = "9ce56ef9-b304-4318-9eba-f0643c1299e0"   # Lakehouse SQL endpoint read by dbt sources; "" = skip refresh
p_lakehouse_id = "f27d086e-be7b-4e40-8e62-ea4d210b4538"      # run artefacts go to its Files/dbt_runs
p_retry_on_failure = 1       # 1 = run `dbt retry` once when the main command fails

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 0. Setup
import base64
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone

import notebookutils
import requests

DBT_CORE_VERSION = "1.12.6"
DBT_FABRIC_VERSION = "1.12.0"
WORK_DIR = "/tmp/dbt_run"
VENV_DIR = f"{WORK_DIR}/venv"
REPO_DIR = f"{WORK_DIR}/repo"
AUTH_DIR = f"{WORK_DIR}/auth"
PROFILES_DIR = f"{WORK_DIR}/profiles"

context = notebookutils.runtime.context
workspace_id = context["currentWorkspaceId"]
run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
shutil.rmtree(WORK_DIR, ignore_errors=True)
for directory in (WORK_DIR, AUTH_DIR, PROFILES_DIR):
    os.makedirs(directory, exist_ok=True)
print(f"Run {run_id} in workspace {workspace_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 1. Isolated dbt environment with pinned versions
subprocess.run([sys.executable, "-m", "venv", VENV_DIR], check=True)
venv_python = f"{VENV_DIR}/bin/python"
dbt_exe = f"{VENV_DIR}/bin/dbt"
subprocess.run(
    [venv_python, "-m", "pip", "install", "--quiet", "--disable-pip-version-check",
     f"dbt-core=={DBT_CORE_VERSION}", f"dbt-fabric=={DBT_FABRIC_VERSION}"],
    check=True,
)
subprocess.run([dbt_exe, "--version"], check=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 2. Shallow clone from Azure DevOps; the token travels only in an HTTP header passed through the environment
ado_token = (
    notebookutils.credentials.getSecret(p_key_vault_url, p_ado_secret_name)
    if p_key_vault_url
    else p_ado_token
)
if not ado_token:
    raise ValueError("Provide p_ado_token (PAT with Code: Read) or p_key_vault_url + p_ado_secret_name.")
auth_header = (
    f"Authorization: Bearer {ado_token}"
    if ado_token.startswith("eyJ")
    else "Authorization: Basic " + base64.b64encode(f":{ado_token}".encode()).decode()
)
git_env = {
    **os.environ,
    "GIT_TERMINAL_PROMPT": "0",
    "GIT_CONFIG_COUNT": "1",
    "GIT_CONFIG_KEY_0": "http.extraHeader",
    "GIT_CONFIG_VALUE_0": auth_header,
}
clone = subprocess.run(
    ["git", "clone", "--quiet", "--depth", "1", "--branch", p_branch, p_repo_url, REPO_DIR],
    env=git_env, capture_output=True, text=True,
)
del ado_token, auth_header, git_env
if clone.returncode != 0:
    raise RuntimeError(f"git clone failed ({clone.returncode}): {clone.stderr.strip()}")

commit = subprocess.run(
    ["git", "-C", REPO_DIR, "rev-parse", "HEAD"], capture_output=True, text=True, check=True
).stdout.strip()
dbt_dir = f"{REPO_DIR}/{p_dbt_subfolder}"
for name in ("dbt_project.yml", "profiles.yml"):
    if not os.path.isfile(f"{dbt_dir}/{name}"):
        raise FileNotFoundError(f"{name} not found in {dbt_dir}")
print(f"Cloned {p_branch} @ {commit[:10]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 3. SQL token of the executing identity -> runtime profile with authentication: token_credential
sql_token = notebookutils.credentials.getToken("https://database.windows.net/")
if not sql_token:
    raise RuntimeError("Fabric did not return a SQL access token.")

with open(f"{AUTH_DIR}/fabric_notebook_credential.py", "w", encoding="utf-8") as f:
    f.write('''import base64, json, os, time
from azure.core.credentials import AccessToken, TokenCredential


class FabricNotebookCredential(TokenCredential):
    def get_token(self, *scopes, **kwargs):
        token = os.environ["FABRIC_SQL_ACCESS_TOKEN"]
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        expires_on = int(json.loads(base64.urlsafe_b64decode(payload))["exp"])
        if expires_on <= int(time.time()):
            raise RuntimeError("SQL token expired; rerun the notebook.")
        return AccessToken(token, expires_on)
''')

with open(f"{AUTH_DIR}/build_profile.py", "w", encoding="utf-8") as f:
    f.write('''import sys
import yaml

source, target, target_name = sys.argv[1:4]
with open(source, encoding="utf-8") as fh:
    profiles = yaml.safe_load(fh)
for profile in profiles.values():
    outputs = profile.get("outputs", {})
    if target_name not in outputs:
        raise SystemExit(f"target {target_name!r} not found in profiles.yml")
    for output in outputs.values():
        for key in ("tenant_id", "client_id", "client_secret", "user", "password", "token", "access_token"):
            output.pop(key, None)
        output.update(
            authentication="token_credential",
            credential_class="fabric_notebook_credential.FabricNotebookCredential",
            credential_kwargs={},
        )
with open(target, "w", encoding="utf-8") as fh:
    yaml.safe_dump(profiles, fh, sort_keys=False)
''')

subprocess.run(
    [venv_python, f"{AUTH_DIR}/build_profile.py", f"{dbt_dir}/profiles.yml",
     f"{PROFILES_DIR}/profiles.yml", p_dbt_target],
    check=True,
)
dbt_env = {
    **os.environ,
    "FABRIC_SQL_ACCESS_TOKEN": sql_token,
    "PYTHONPATH": os.pathsep.join(filter(None, [AUTH_DIR, os.environ.get("PYTHONPATH")])),
}
print(f"Runtime profile written for target '{p_dbt_target}'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 4. Make the latest Landing Delta versions visible through the SQL endpoint before dbt reads them
if p_sql_endpoint_id:
    headers = {"Authorization": f"Bearer {notebookutils.credentials.getToken('pbi')}"}
    url = (f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
           f"/sqlEndpoints/{p_sql_endpoint_id}/refreshMetadata")
    response = requests.post(url, headers=headers, json={}, timeout=60)
    if response.status_code == 202:
        location = response.headers["Location"]
        state = {}
        for _ in range(60):
            time.sleep(int(response.headers.get("Retry-After", 5)))
            state = requests.get(location, headers=headers, timeout=60).json()
            if state.get("status") in ("Succeeded", "Failed", "Cancelled"):
                break
        if state.get("status") != "Succeeded":
            raise RuntimeError(f"SQL endpoint metadata refresh ended with {state}")
        response = requests.get(f"{location}/result", headers=headers, timeout=60)
    response.raise_for_status()
    failed = [t for t in response.json().get("value", []) if t.get("status") == "Failure"]
    if failed:
        raise RuntimeError(f"SQL endpoint sync failed for: {failed}")
    print("SQL endpoint metadata refreshed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# 5. Run dbt, keep the artefacts even on failure, and fail the activity on error
def run_dbt(*arguments):
    command = [dbt_exe, *arguments, "--project-dir", dbt_dir,
               "--profiles-dir", PROFILES_DIR, "--target", p_dbt_target]
    print(">", " ".join(command))
    return subprocess.run(command, env=dbt_env).returncode


status = "failed"
try:
    if run_dbt("debug") != 0:
        raise RuntimeError("dbt debug failed: check the executing identity's Warehouse permissions.")
    if any(os.path.isfile(f"{dbt_dir}/{name}") for name in ("packages.yml", "dependencies.yml")):
        if run_dbt("deps") != 0:
            raise RuntimeError("dbt deps failed.")
    return_code = run_dbt(p_dbt_command, *shlex.split(p_dbt_args))
    if return_code != 0 and p_retry_on_failure:
        print("dbt failed; retrying failed and skipped nodes once in 30 seconds...")
        time.sleep(30)
        return_code = run_dbt("retry")
    status = "success" if return_code == 0 else "failed"
finally:
    destination = (f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{p_lakehouse_id}"
                   f"/Files/dbt_runs/{run_id}_{p_dbt_command}")
    run_info = {
        "run_id": run_id,
        "status": status,
        "branch": p_branch,
        "commit": commit,
        "target": p_dbt_target,
        "command": p_dbt_command,
        "args": p_dbt_args,
        "executed_by": context.get("userName"),
        "dbt_core": DBT_CORE_VERSION,
        "dbt_fabric": DBT_FABRIC_VERSION,
    }
    notebookutils.fs.put(f"{destination}/run_info.json", json.dumps(run_info, indent=2), True)
    for relative in ("target/run_results.json", "logs/dbt.log"):
        if os.path.isfile(f"{dbt_dir}/{relative}"):
            notebookutils.fs.cp(f"file:{dbt_dir}/{relative}", f"{destination}/{os.path.basename(relative)}")
    print(f"Artefacts: Files/dbt_runs/{run_id}_{p_dbt_command}")

if status != "success":
    run_results = f"{dbt_dir}/target/run_results.json"
    failed_nodes = []
    if os.path.isfile(run_results):
        with open(run_results, encoding="utf-8") as fh:
            failed_nodes = [f"{r['unique_id']}: {r['status']}" for r in json.load(fh)["results"]
                            if r["status"] in ("error", "fail")]
    raise RuntimeError(f"dbt {p_dbt_command} failed: " + ("; ".join(failed_nodes[:20]) or "see dbt.log"))
print(f"dbt {p_dbt_command} succeeded @ {commit[:10]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
