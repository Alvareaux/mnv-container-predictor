#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base
from datetime import datetime


class Predictor:
    type = None

    def run(self, index: str, from_date: datetime, to_date: datetime, predict_to: datetime):
        ...
