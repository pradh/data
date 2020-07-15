import re
import json
import logging
import os
import subprocess
import tempfile

import requests

from app import configs
from app.service import github_api
from app.service import gcs_io
from app import utils


def parse_manifest(path):
    with open(path, 'r') as file:
        return json.load(file)


def create_venv(requirements_path, venv_dir):
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.sh') as bash_script:
        create_template = 'python3 -m virtualenv --verbose --clear {}\n'
        activate_template = '. {}/bin/activate\n'
        pip_template = 'python3 -m pip install --verbose --no-cache-dir --requirement {}\n'
        bash_script.write(create_template.format(venv_dir))
        bash_script.write(activate_template.format(venv_dir))
        bash_script.write(pip_template.format(requirements_path))
        bash_script.flush()

        process = subprocess.run(
            ['bash', bash_script.name], check=True, capture_output=True)
    return os.path.join(venv_dir, 'bin/python'), process


def run_user_script(script_path, interpreter_path):
    return subprocess.run([interpreter_path, script_path], check=True, capture_output=True)


def execute_import_on_update(absolute_import_name):
    logging.info(absolute_import_name + ': BEGIN')
    github = github_api.GitHubRepoAPI()
    with tempfile.TemporaryDirectory() as tmpdir:
        logging.info(absolute_import_name + ': downloading repo')
        repo_dirname = github.download_repo(tmpdir)
        logging.info(absolute_import_name + ': downloaded repo ' + repo_dirname)
        cwd = os.path.join(tmpdir, repo_dirname)
        os.chdir(cwd)

        dir_path, import_name = utils.split_relative_import_name(
            absolute_import_name)
        manifest_path = os.path.join(dir_path, configs.MANIFEST_FILENAME)
        logging.info(absolute_import_name + ': PARSE manifest ' + manifest_path)
        manifest = parse_manifest(manifest_path)
        for spec in manifest['import_specifications']:
            if import_name == 'all' or import_name == spec['import_name']:
                import_one(dir_path, spec)

    logging.info(absolute_import_name + ': END')
    return 'success'


def import_one(dir_path, import_spec):
    import_name = import_spec['import_name']

    cwd = os.getcwd()
    os.chdir(dir_path)

    urls = import_spec.get('data_download_url')
    if urls:
        for url in urls:
            logging.info(import_name + ': DOWNLOADING ' + url)
            utils.download_file(url, '.')

    with tempfile.TemporaryDirectory() as tmpdir:
        logging.info(import_name + ': CREATING venv')
        interpreter_path, process = create_venv(configs.REQUIREMENTS_FILENAME, tmpdir)

        script_paths = import_spec.get('scripts')
        for path in script_paths:
            logging.info(import_name + ': INVOKING script ' + path)
            process = run_user_script(path, interpreter_path)

    import_inputs = import_spec.get('import_inputs', [])
    for import_input in import_inputs:
        template_mcf = import_input.get('template_mcf')
        cleaned_csv = import_input.get('cleaned_csv')
        node_mcf = import_input.get('node_mcf')

        time = utils.utctime()
        if template_mcf:
          logging.info(import_name + ': UPLOADING template_mcf ' + template_mcf)
          gcs_io.upload_file(template_mcf, f'{dir_path}:{import_name}/{time}/{os.path.basename(template_mcf)}', configs.BUCKET_NAME)

        if cleaned_csv:
          logging.info(import_name + ': UPLOADING cleaned_csv ' + cleaned_csv)
          gcs_io.upload_file(cleaned_csv, f'{dir_path}:{import_name}/{time}/{os.path.basename(cleaned_csv)}', configs.BUCKET_NAME)

        if node_mcf:
          logging.info(import_name + ': UPLOADING node_mcf ' + node_mcf)
          gcs_io.upload_file(node_mcf, f'{dir_path}:{import_name}/{time}/{os.path.basename(node_mcf)}', configs.BUCKET_NAME)

    os.chdir(cwd)
