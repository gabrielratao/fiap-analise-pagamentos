# src/config/settings.py

import yaml


class Settings:

    def carregar_config(self, path: str = "./config/settings.yaml") -> dict:
        """Carrega um arquivo de configuração YAML."""
        with open(path, "r", encoding="utf-8") as file:
            return yaml.safe_load(file)
