#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
import logging
from datetime import datetime, timedelta
from collections import defaultdict

# Internal
from service_predictor.service.predictor._predictor import Predictor
from service_predictor.service.connection import ConnectionElastic, ConnectionSQL

# External
from tqdm import tqdm
import pandas as pd
import numpy as np
from prophet import Prophet


class TelegramPredictorMetrics(Predictor):
    type = 'telegram'

    progress_bar = False

    # Metrics to predict
    metrics = [
        'views',
    ]

    # Fields to save from Elasticsearch
    __fields = [
        'id',

        'chat_id',
        'message_id',

        'delta',

        'date',
        'loading_date',
    ]

    # Minimum articles to predict on
    minimum_articles = 100

    # Quantile to cut outliers for better model fit (no anomalies in the train data)
    cut_quantile = 0.95

    # Prediction step (minutes between two predictions)
    step = 5

    def __init__(self, connection_elastic: ConnectionElastic, connection_sql: ConnectionSQL | None = None):
        """
        Telegram predictor

        :param connection_elastic: ConnectionElastic object
        :param connection_sql: ConnectionSQL object
        """

        # Disable annoying logging from Prophet and cmdstanpy
        logging.getLogger("prophet").setLevel(logging.WARNING)
        logging.getLogger("cmdstanpy").disabled = True

        self.__connection_elastic = connection_elastic
        self.__connection_sql = connection_sql

    def run(self, index: str, from_date: datetime, to_date: datetime, predict_to: datetime):
        """
        Grabs articles and create predictions for given index and dates

        :param index: Elastic index (only with supported data)
        :param from_date: Starting date of train data
        :param to_date: Ending date of train data and starting date for prediction
        :param predict_to: Ending date of prediction
        :return:
        """
        articles = self.__connection_elastic.get_articles(
            index=index,
            from_date=from_date,
            to_date=to_date,
            fields=self.__fields + self.metrics
        )

        grouped_articles = self._group_articles(articles)
        del articles

        predictions = self._predict_all(grouped_articles, to_date, predict_to)
        return predictions

    def _group_articles(self, articles: list[dict]) -> dict[dict[list[dict]]]:
        """
        Group articles by chat_id and delta

        :param articles: List of articles
        :return: Dict with articles
        """

        grouped_articles = defaultdict(lambda: defaultdict(list))

        for article in articles:
            grouped_articles[article['chat_id']][article['delta']].append(article)

        return grouped_articles

    def _predict_all(self, grouped_articles: dict[dict[list[dict]]], predict_from: datetime, predict_to: datetime) \
            -> list[dict]:
        """
        Predict all metrics for all articles in grouped_articles

        :param grouped_articles: Grouped articles dict
        :param predict_from: Prediction start date
        :param predict_to: Prediction end date
        :return:
        """

        # Create future dataframe
        delta_minutes = int(np.ceil((predict_to - predict_from).total_seconds() / 60))

        future_df = pd.DataFrame([
            predict_from.replace(minute=0, second=0) +
            timedelta(minutes=x) for x in range(0, delta_minutes + 1, self.step)
        ], columns=['ds'])

        # Predict for each chat_id and delta pair
        predictions = []

        for chat_id in tqdm(grouped_articles, disable=not self.progress_bar, desc='Predicting'):
            for delta in grouped_articles[chat_id]:
                articles = grouped_articles[chat_id][delta]

                if len(articles) < self.minimum_articles:
                    logging.info(f'Not enough articles for chat_id {chat_id} and delta {delta}, skipping...')
                    continue

                prediction = self._predict_metrics(articles, future_df)

                for row in prediction:
                    predictions.append({
                        'chat_id': chat_id,
                        'delta': delta,
                        'date': row,

                        **prediction[row]
                    })

        return predictions

    def _predict_metrics(self, articles: list[dict], future_df: pd.DataFrame) -> dict:
        """
        Predict all metrics for given articles and future dataframe

        :param articles: List of articles to fit model on
        :param future_df: List of dates to predict on
        :return: Prediction dict
        """

        prediction = defaultdict(dict)

        for metric in self.metrics:
            # Prepare data
            data = [[article['date'], article[metric]] for article in articles]
            df = pd.DataFrame(data, columns=['ds', 'y'])

            # Cut biggest outliers to not screw up the model
            df = df[df['y'] < df['y'].quantile(self.cut_quantile)]

            # Fit model
            model = Prophet()
            model.fit(df)

            # Predict
            forecast = model.predict(future_df)

            for index, row in forecast.iterrows():
                prediction[row['ds']].update({
                    metric: row['yhat'],
                    f'{metric}_lower': row['yhat_lower'],
                    f'{metric}_upper': row['yhat_upper']
                })

        return prediction
