# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "lh_oyster_path"
# MAGIC     }
# MAGIC }

# CELL ********************

# Fabric notebook source

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Fabric GitHub dbt runner with notebook user token
# Runtime install, GitHub clone, Fabric SQL token, runtime profile, then dbt execution.
# 
# Pipeline execution uses the activity's configured identity; by default, the pipeline's last modified user.
# Clicking Run alone does not switch that identity to the caller. Use a user connection and save the pipeline as the intended user.
# That identity needs Warehouse DDL/DML permissions and SELECT access to the Lakehouse SQL sources.
# No service principal secrets or Key Vault access are required for this user-token runner.
# Repository cloning without a GitHub credential requires a public repository.
# > If the issued user token is not accepted by the Warehouse SQL endpoint, `dbt debug` stops with an authentication/authorization error.
# # If the main dbt command fails, the runner waits 30 seconds and runs `dbt retry` once for failed and skipped nodes (e.g. transient Warehouse write conflicts).


# PARAMETERS CELL ********************

# ---- FABRIC PIPELINE PARAMETERS ----
repo_url = globals().get('repo_url', 'https://github.com/MiklosJeneiDezerius/demo_oyster_test.git')
branch = globals().get('branch', 'main')
dbt_subfolder = globals().get('dbt_subfolder', 'dbt_oyster_demo_test_core')
dbt_command = globals().get('dbt_command', 'build')
dbt_args = globals().get('dbt_args', '')  # pl. '--select b_oys_customer'
dbt_server = globals().get('dbt_server', '')  # Warehouse SQL endpoint hostname.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import os
import sys
import shutil
import shlex
import subprocess
import textwrap
import time
import zipfile
from notebookutils import credentials

if not dbt_server:
    raise ValueError('Set dbt_server to the Warehouse SQL endpoint hostname.')

# 1. Isolated dbt environment. No notebook-kernel restart.
venv_dir = '/tmp/dbt_venv'
if os.path.exists(venv_dir):
    shutil.rmtree(venv_dir)
print('Creating isolated Python environment...')
subprocess.run([sys.executable, '-m', 'venv', venv_dir], check=True)
venv_python = os.path.join(venv_dir, 'bin', 'python')
dbt_exe = os.path.join(venv_dir, 'bin', 'dbt')
print('Installing dbt-fabric...')
subprocess.run([
    venv_python, '-m', 'pip', 'install', '--quiet',
    'dbt-fabric==1.11.1', 'dbt-core<1.12',
], check=True)

# 2. Extract the deployed dbt project from the default Lakehouse.
dbt_dir = '/tmp/oyster_path_core_dbt'
if os.path.exists(dbt_dir):
    shutil.rmtree(dbt_dir)
os.makedirs(dbt_dir, exist_ok=True)
with zipfile.ZipFile('/lakehouse/default/Files/dbt/oyster-path-core-dbt.zip') as archive:
    archive.extractall(dbt_dir)

# 3. Validate dbt project and profile.
project_file = os.path.join(dbt_dir, 'dbt_project.yml')
source_profiles_file = os.path.join(dbt_dir, 'profiles.yml')
if not os.path.isfile(project_file):
    raise FileNotFoundError(f'dbt_project.yml not found: {project_file}')
if not os.path.isfile(source_profiles_file):
    raise FileNotFoundError(f'profiles.yml not found: {source_profiles_file}')

# 4. Acquire current Fabric notebook user's SQL token.
print('Acquiring SQL access token from Fabric notebook session...')
try:
    sql_token = credentials.getToken('https://database.windows.net/')
except Exception as error:
    raise RuntimeError('SQL token acquisition failed. Check the notebook activity user connection and execution identity.') from error
if not sql_token:
    raise RuntimeError('Fabric did not return a SQL access token.')
print('SQL access token acquired.')

# 5. Create importable Azure TokenCredential module.
credential_module = os.path.join(dbt_dir, 'fabric_notebook_credential.py')
credential_source = r'''import base64
import json
import os
import time
from azure.core.credentials import AccessToken, TokenCredential

class FabricNotebookCredential(TokenCredential):
    def get_token(self, *scopes, **kwargs):
        token = os.environ.get('FABRIC_SQL_ACCESS_TOKEN')
        if not token:
            raise RuntimeError('FABRIC_SQL_ACCESS_TOKEN is missing.')
        try:
            payload = token.split('.')[1]
            payload += '=' * (-len(payload) % 4)
            expires_on = int(json.loads(base64.urlsafe_b64decode(payload))['exp'])
        except (ValueError, KeyError, IndexError, TypeError) as error:
            raise RuntimeError('SQL token has no valid expiry claim.') from error
        if expires_on <= int(time.time()):
            raise RuntimeError('SQL token expired. Restart the notebook run to acquire a fresh token.')
        return AccessToken(token, expires_on)
'''
with open(credential_module, 'w', encoding='utf-8') as f:
    f.write(textwrap.dedent(credential_source).lstrip())

# 6. Copy profiles.yml and change all outputs to token_credential.
runtime_profiles_dir = '/tmp/dbt_profiles'
if os.path.exists(runtime_profiles_dir):
    shutil.rmtree(runtime_profiles_dir)
os.makedirs(runtime_profiles_dir, exist_ok=True)
runtime_profiles_file = os.path.join(runtime_profiles_dir, 'profiles.yml')
profile_builder = '/tmp/prepare_dbt_profile.py'
profile_builder_source = r'''import sys
import yaml
source_file, target_file = sys.argv[1], sys.argv[2]
with open(source_file, 'r', encoding='utf-8') as f:
    profiles = yaml.safe_load(f)
if not isinstance(profiles, dict) or not profiles:
    raise RuntimeError('profiles.yml is empty or invalid')
for profile in profiles.values():
    if not isinstance(profile, dict):
        continue
    for output in profile.get('outputs', {}).values():
        if not isinstance(output, dict):
            continue
        output['authentication'] = 'token_credential'
        output['credential_class'] = 'fabric_notebook_credential.FabricNotebookCredential'
        output['credential_kwargs'] = {}
        for key in ('tenant_id','client_id','client_secret','user','password','token','access_token'):
            output.pop(key, None)
with open(target_file, 'w', encoding='utf-8') as f:
    yaml.safe_dump(profiles, f, sort_keys=False)
'''
with open(profile_builder, 'w', encoding='utf-8') as f:
    f.write(textwrap.dedent(profile_builder_source).lstrip())
subprocess.run([venv_python, profile_builder, source_profiles_file, runtime_profiles_file], check=True)
print(f'Runtime profile created: {runtime_profiles_file}')

# 7. Pass token and custom module only to dbt subprocess.
dbt_environment = os.environ.copy()
dbt_environment['FABRIC_SQL_ACCESS_TOKEN'] = sql_token
dbt_environment['DBT_SERVER'] = dbt_server
existing_pythonpath = dbt_environment.get('PYTHONPATH', '')
dbt_environment['PYTHONPATH'] = dbt_dir if not existing_pythonpath else dbt_dir + os.pathsep + existing_pythonpath

# 8. Run dbt.
def run_dbt(arguments):
    command = [dbt_exe, *arguments, '--project-dir', dbt_dir, '--profiles-dir', runtime_profiles_dir]
    print('>', ' '.join(command))
    subprocess.run(command, check=True, env=dbt_environment)

run_dbt(['debug'])
if any(os.path.isfile(os.path.join(dbt_dir, name)) for name in ('packages.yml', 'dependencies.yml')):
    run_dbt(['deps'])
try:
    run_dbt([dbt_command, *shlex.split(dbt_args)])
except subprocess.CalledProcessError:
    # Fabric Warehouse rolls back one side of a concurrent write conflict; rerun only failed and skipped nodes once.
    retry_delay_seconds = 30
    print(f'dbt {dbt_command} failed. Retrying failed and skipped nodes in {retry_delay_seconds} seconds...')
    time.sleep(retry_delay_seconds)
    run_dbt(['retry'])
print('dbt execution completed successfully.')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
