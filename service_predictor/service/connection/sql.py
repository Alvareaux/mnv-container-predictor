#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Base


# Internal

# External
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import insert


class ConnectionSQL:
    def __init__(self, login, password, hostname, db):
        """
        Database connection object
        :param login: User login
        :param password: User password
        :param hostname: Address of the database
        :param db: Database name
        """

        self._engine = create_engine(f'mysql://{login}:{password}@{hostname}/{db}')

        self._metadata = MetaData()
        self._metadata.reflect(self._engine)

        session = sessionmaker(bind=self._engine)

        self.__session = session()

    def upsert_data(self, table_name: str, data: list[dict]):
        """
        Upload data to SQL database

        :param table_name: Table to upload predictions to
        :param data: List of dicts to upload
        :return:
        """

        # God is dead and this code is proof of it

        # Recreate table class from metadata reflection or we cant use on_duplicate_key_update statement
        Base = declarative_base()

        class DataTable(Base):
            __table__ = Table(table_name, self._metadata, autoload=True, autoload_with=self._engine)

        assert all([column in DataTable.__table__.columns.keys() for column in data[0].keys()]), \
            'Wrong columns in predictions'

        insert_statement = insert(DataTable).values(data)
        columns = [column.name for column in insert_statement.inserted]

        # Column name fix for MySQL upsert statement,
        # cause sqlalchemy used inserted.column_name instead of VALUES(column_name)
        # and it also dont work with text() for some reason
        upsert_statement = insert_statement.on_duplicate_key_update({x: text(f'VALUES({x})') for x in columns})

        self.__session.execute(upsert_statement)
        self.__session.commit()
