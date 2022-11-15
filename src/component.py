'''
Template Component main class.

'''

import csv
import datetime
import logging
import os
import sys
from kbc.env_handler import KBCEnvHandler
from pathlib import Path

from kbc_scripts import kbcapi_scripts

# configuration variables
KEY_SRC_TOKEN = '#src_token'
PAR_CONFIG_LISTS = 'configs.csv'
KEY_API_TOKEN = '#api_token'
KEY_REGION = 'aws_region'
KEY_DST_REGION = 'dst_aws_region'
# #### Keep for debug
KEY_DEBUG = 'debug'

MANDATORY_PARS = [KEY_API_TOKEN, KEY_SRC_TOKEN, KEY_REGION, KEY_DST_REGION]
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        # for easier local project setup
        default_data_dir = Path(__file__).resolve().parent.parent.joinpath('data').as_posix() \
            if not os.environ.get('KBC_DATADIR') else None

        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO,
                               data_path=default_data_dir)
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)
        self.storage_tokens = dict()

        # get other stacks from image context

        kbcapi_scripts.URL_SUFFIXES = {**kbcapi_scripts.URL_SUFFIXES, **self.image_params}

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa
        configs_path = os.path.join(self.tables_in_path, PAR_CONFIG_LISTS)
        out_file_path = os.path.join(self.tables_out_path, 'transferred_configs_log.csv')
        src_region = params[KEY_REGION]
        dst_region = params[KEY_DST_REGION]
        if not os.path.exists(configs_path):
            logging.exception(f'The table {PAR_CONFIG_LISTS} must be on input!')

        with open(configs_path, mode='rt', encoding='utf-8') as in_file, open(out_file_path, mode='w+',
                                                                              encoding='utf-8') as out_file:
            reader = csv.DictReader(in_file, lineterminator='\n')
            writer = csv.DictWriter(out_file,
                                    fieldnames=['project_id', 'region', 'src_cfg_id', 'dst_cfg_id', 'component_id',
                                                'time'], lineterminator='\n')
            writer.writeheader()

            for cfg in reader:
                project_id = cfg['project_id']
                token = self._get_project_storage_token(params[KEY_API_TOKEN], project_id, region=dst_region)
                logging.info(
                    f'Transferring {cfg["component_id"]} cfg {cfg["configuration_id"]} '
                    f'into project {cfg["project_id"]}')
                if cfg['component_id'] != 'orchestrator-legacy':
                    result_id = cfg['configuration_id']
                    if cfg['component_id'] == 'flow':
                        cfg['component_id'] = 'keboola.orchestrator'

                    transferred = kbcapi_scripts.migrate_configs(params[KEY_SRC_TOKEN], token['token'],
                                                                 cfg['configuration_id'],
                                                                 cfg['component_id'],
                                                                 src_region=src_region,
                                                                 dst_region=dst_region,
                                                                 use_src_id=True, fail_on_existing=False)

                else:
                    o = kbcapi_scripts.clone_orchestration(params[KEY_SRC_TOKEN], token['token'], src_region,
                                                           dst_region, cfg['configuration_id'])
                    result_id = o['id']
                    transferred = True

                if transferred:
                    writer.writerow({'project_id': project_id,
                                     'region': 'EU',
                                     'src_cfg_id': cfg['configuration_id'],
                                     'dst_cfg_id': result_id,
                                     'component_id': cfg['component_id'],
                                     'time': datetime.datetime.utcnow().isoformat()})

        self.configuration.write_table_manifest(out_file_path,
                                                primary_key=['project_id', 'region', 'src_cfg_id', 'dst_cfg_id',
                                                             'component_id'], incremental=True)
        logging.info("Done!")

    def _get_project_storage_token(self, manage_token, project_id, region='EU'):
        project_pk = f'{region}-{project_id}'
        if not self.storage_tokens.get(project_pk):
            logging.info(f'Generating token for project {region}-{project_id}')
            self.storage_tokens[project_pk] = kbcapi_scripts.generate_token('Sample Config provisioning', manage_token,
                                                                            project_id, region, manage_tokens=True)
        return self.storage_tokens[project_pk]


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        comp = Component(debug_arg)
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(1)
