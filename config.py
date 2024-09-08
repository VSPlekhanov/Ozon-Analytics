import os
import json

class Config:
    def __init__(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, 'config/config.json')

        with open(config_path, 'r') as f:
            self.config = json.load(f)

        self.ozon_api_key_file = os.path.join(base_dir, self.config['ozon_api_key_file'])
        self.client_id_file = os.path.join(base_dir, self.config['client_id_file'])
        self.google_sheet_credentials = os.path.join(base_dir, self.config['google_sheet_credentials'])
        self.output_directory = os.path.join(base_dir, self.config['output_directory'])

    def get_ozon_token(self):
        with open(self.ozon_api_key_file, 'r') as f:
            return f.read().strip()

    def get_client_id(self):
        with open(self.client_id_file, 'r') as f:
            return f.read().strip()

    def get_google_credentials(self):
        return self.google_sheet_credentials

    def get_output_directory(self):
        return self.output_directory
