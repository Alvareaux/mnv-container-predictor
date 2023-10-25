#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
from datetime import datetime

# Internal

# External
from tqdm import tqdm
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


class ConnectionElastic:
    progress_bar = None

    def __init__(self, elastic_url, elastic_api_key):
        """
        Elastic connection

        :param elastic_url: Elasticsearch URL
        :param elastic_api_key: Elasticsearch API key
        """

        self.__elastic_url = elastic_url
        self.__elastic_api_key = elastic_api_key

    def get_articles(self, index: str, from_date: datetime, to_date: datetime, fields: list | None) -> list[dict]:
        """
        Get articles from Elasticsearch

        :param index: Index name
        :param from_date: From date
        :param to_date: To date
        :param fields: Fields to get from Elasticsearch (None = all)
        :return: List of articles
        """

        # Get articles from Elasticsearch
        articles = []

        with Elasticsearch(self.__elastic_url, api_key=self.__elastic_api_key, request_timeout=3600) as elastic:
            # Get articles from Elasticsearch
            for article in tqdm(scan(elastic, index=index, source_includes=fields, query={
                'query': {
                    'range': {
                        'loading_date': {
                            'gte': from_date.strftime('%Y-%m-%dT%H:%M:%S'),
                            'lte': to_date.strftime('%Y-%m-%dT%H:%M:%S')
                        }
                    }
                }
            }), disable=not self.progress_bar, desc='Getting articles from Elasticsearch'):
                articles.append(article['_source'])

        return articles