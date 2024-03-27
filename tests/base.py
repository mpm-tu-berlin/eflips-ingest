import inspect
import os
import pathlib
from abc import ABC, abstractmethod
from enum import Enum

import pytest
from eflips.model import setup_database, Base
from sqlalchemy import create_engine

from eflips.ingest.base import AbstractIngester


class BaseIngester(ABC):
    """

    Abstract base class for the ingester tests. Contains some common methods and attributes.

    """

    @pytest.fixture(autouse=True)
    def setup_database(self) -> None:
        """
        Setup the database for the tests.

        :return: None. The database URL is stored in the `database_url` attribute.
        """
        if not "DATABASE_URL" in os.environ:
            raise ValueError("DATABASE_URL environment variable not set.")
        self.database_url = os.environ.get("DATABASE_URL")
        engine = create_engine(self.database_url)
        Base.metadata.drop_all(engine)
        setup_database(engine)

    @abstractmethod
    @pytest.fixture()
    def ingester(self) -> AbstractIngester:
        """
        Setup the ingester for the tests.

        This method should be implemented by the subclass. The self.database_url attribute should be used to create the
        ingester.

        :return: An instance of the ingester.
        """
        pass

    @abstractmethod
    @pytest.mark.skip("This is an abstract test class.")
    def test_prepare(self, ingester) -> None:
        """
        Test the meth:`prepare` method of the ingester.

        This should be subclassed by the subclass. Please test the "success" case as well as the proper generation
        of error messages for the "failure" case.

        :param ingester: An instance of the ingester.
        :return: Nothing
        """

    @abstractmethod
    @pytest.mark.skip("This is an abstract test class.")
    def test_ingest(self, ingester) -> None:
        """
        Test the meth:`ingest` method of the ingester.

        First, the prepare method should be called with valid data. Then *a new ingester* should be created and the
        ingest method should be called with the UUID returned by the prepare method. At this point, no exceptions should
        be raised.

        The ingester must call session.commit() at the end of the ingest method if it is using SQLAlchemy, as the
        session is not committed automatically.

        :param ingester: An instance of the ingester.
        :return: Nothing
        """

    def test_prepare_params(self, ingester) -> None:
        """
        This method makes sure that the `prepare` method parameter's types are valid.

        Allowed types are:
        - str: For text data.
        - int: For integer data.
        - float: For floating point data.
        - bool: For boolean data.
        - subclass of Enum: For enumerated data. Any subclass of Enum is allowed.
        - pathlib.Path: For file paths. *This is what should be done to express the need for uploaded files.*

        :return: None
        """
        # Obtain parameter names and types using introspection
        sig = inspect.signature(ingester.prepare)
        for param in sig.parameters.values():
            if param.name == "progress_callback":
                continue
            if param.annotation in [str, int, float, bool, pathlib.Path]:
                continue
            elif issubclass(param.annotation, Enum):
                continue
            else:
                raise AssertionError(f"Invalid parameter type {param.annotation} for parameter {param.name}.")

    def test_prepare_param_names(self, ingester) -> None:
        """
        This method tests the meth:`prepare_param_names` property. It makes sure that for each parameter in the
        `prepare` method, there is a corresponding parameter in the `prepare_param_names` property.

        :return: None
        """

        # Obtain parameter names and types using introspection
        sig = inspect.signature(ingester.prepare)
        for param in sig.parameters.values():
            if param.name == "progress_callback":
                continue
            assert param.name in ingester.prepare_param_names.keys()
            if issubclass(param.annotation, Enum):
                assert isinstance(ingester.prepare_param_names[param.name], dict)
                for value in param.annotation:
                    assert value in ingester.prepare_param_names[param.name].keys()
            else:
                assert isinstance(ingester.prepare_param_names[param.name], str)

    def test_prepare_param_description(self, ingester) -> None:
        """
        This method tests the meth:`prepare_param_description` property. It makes sure that for each parameter in the
        `prepare` method, there is a corresponding parameter in the `prepare_param_description` property.

        :return: None
        """

        # Obtain parameter names and types using introspection
        sig = inspect.signature(ingester.prepare)
        for param in sig.parameters.values():
            if param.name == "progress_callback":
                continue
            assert param.name in ingester.prepare_param_description.keys()
            if issubclass(param.annotation, Enum):
                assert isinstance(ingester.prepare_param_description[param.name], dict)
                for value in param.annotation:
                    assert value in ingester.prepare_param_description[param.name].keys()
            else:
                assert isinstance(ingester.prepare_param_description[param.name], str)
