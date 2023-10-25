#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
import json

# Import from project
from pathlib import Path
import sys

working_dir = Path(__file__).parents[2].resolve()
sys.path.append(str(working_dir))

# Internal
from service_predictor.service import Predictor

# External
from mnv_data_container_template.container_template import Container
from mnv_data_package_database.queries.tasker import TaskerQueries


class ContainerPredictor(Container):
    __version__ = '1.p.0'

    _service_name = 'predictor'

    def __init__(self):
        """
        Grabber container object

        """

        # Required config groups for container
        self._config_services_groups = ['elasticsearch']

        # Setup params
        self._setup_pubsub = False
        self._setup_rabbitmq = False
        self._setup_versioning = False

        super().__init__()

    def _setup_container(self):
        """
        Additional setup for grabber container

        :return:
        """

        # Setup Elasticsearch
        self._setup_elastic()

    def _setup_elastic(self):
        """
        Setup Elastic connection

        """

        self.elastic_url = self._service_values['elasticsearch_url']
        self.__elastic_api_key = self._gcloud_secrets.get_secret('pipeline-data-key-elasticsearch')

    def run(self):
        """
        Start grabbing tasks

        """

        ...


if __name__ == '__main__':
    processor = ContainerPredictor()
    processor.run()
