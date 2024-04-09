import os
import tempfile
from abc import abstractmethod, ABC
from enum import Enum
from pathlib import Path
from typing import Tuple, Dict, Callable
from uuid import UUID


class AbstractIngester(ABC):
    def __init__(self, database_url: str):
        """
        Constructor for the BaseIngester class.

        :param database_url: A string representing the URL of the database to ingest into. Must be of the form
                             postgresql://user:password@host:port/database. An url of the format
                             postgis://user:password@host:port/database (django style) will be converted to the4
        :return: None
        """
        if database_url.startswith("postgis://"):
            database_url = database_url.replace("postgis://", "postgresql://", 1)
        self.database_url = database_url

    @abstractmethod
    def prepare(
        self,
        progress_callback: None | Callable[[float], None] = None,
        **kwargs: Dict[str, str | int | float | bool | Enum | Path],
    ) -> Tuple[bool, UUID | Dict[str, str]]:
        """
        Prepare and validate the input data for ingestion.

        The keyword arguments should be set to specific arguments when subclassing this method. Additionally, the
        :meth:`prepare_param_names` and :meth:`prepare_param_description` methods should be implemented to provide hints
        for the parameters of this method.

        This method of subclassing vilotaes the Liskov Substitution Principle, as the subclassed method has a different
        signature than the superclass method. As such, the subclass should be marked with # type: ignore.

        The types for keyword arguments should be limited to the following:
        - str: For text data.
        - int: For integer data.
        - float: For floating point data.
        - bool: For boolean data.
        - subclass of Enum: For enumerated data.
        - pathlib.Path: For file paths. *This is what should be done to express the need for uploaded files.*

        Additionally the method should accept a progress_callback parameter, which is a function that accepts a float
        value between 0 and 1. This function should be called periodically to update the progress of the ingestion
        process.

        When developing a (web) interface, it is suggested to use introspection on the parameters of this method to
        generate a form for the user to fill in. This can be done by using the :meth:`inspect.signature` method from the
        `inspect` module.

        This method should validate the input data and save its working data to a temporary location (suggested to be
        the path returned by the :meth:`path_for_uuid` method). If the data is valid, it should return a UUID
        representing the data and a boolean indicating that the data is valid. If the data is invalid, it should return
        a dictionary containing the error names and messages.

        :param kwargs: A dictionary containing the input data.
        :return: A tuple containing a boolean indicating whether the input data is valid and either a UUID or a dictionary
                 containing the error message.
        """
        pass

    @abstractmethod
    def ingest(self, uuid: UUID, progress_callback: None | Callable[[float], None] = None) -> None:
        """
        Ingest the data into the database. In order for this method to be called, the :meth:`prepare` method must have
        returned a UUID, indicating that the preparation was successful.

        This method must call the progress_callback function periodically to update the progress of the ingestion
        process. The progress_callback function should accept a float value between 0 and 1.

        The method should check if a scenario with the same UUID already exists in the database. If it does, it should
        add the data to the existing scenario. If it does not, it should create a new scenario.

        :param uuid: A UUID representing the data to ingest.
        :return: Nothing. If unexpected errors occur, they should be raised as exceptions.
        """
        pass

    @classmethod
    @abstractmethod
    def prepare_param_names(cls) -> Dict[str, str | Dict[Enum, str]]:
        """
        A dictionary containing the parameter names for :meth:`prepare`.

        These should be short, descriptive names for the parameters of the :meth:`prepare` method. The keys must be the
        names of the parameters, and the values should be strings describing the parameter. If the keyword argument is
        an enumerated type, the value should be a dictionary with the keys being the members.

        This method can then be used to generate a help text for the user.

        :return: A dictionary containing the parameter hints for the prepare method.
        """
        pass

    @classmethod
    @abstractmethod
    def prepare_param_description(cls) -> Dict[str, str | Dict[Enum, str]]:
        """
        A dictionary containing the parameter descriptions for :meth:`prepare`.

        These should be longer, more detailed descriptions of the parameters of the :meth:`prepare`
        method. The keys must be the names of the parameters, and the values should be strings describing the parameter.

        This method can then be used to generate a help text for the user.

        :return: A dictionary containing the parameter hints for the prepare method.
        """
        pass

    def path_for_uuid(self, uuid: UUID) -> Path:
        """
        Generate a path in the temporary directory for a given UUID.

        This is the recommended way to store temporary data for the ingestion process. Note that this only returns the
        path, it does not create the directory or file.

        :param uuid: A UUID.
        :return: A Path object representing the path for the given UUID.
        """
        temp_dir = tempfile.gettempdir()
        return Path(os.path.join(temp_dir, f"{uuid}"))
