from .components.access import AccessManager
from .components.dataset import DatasetManager
from .components.file import FileManager
from .components.request import RequestManager
from .components.logging_conf import configure_logger

from .datastore.datastore import Datastore

configure_logger(AccessManager)
configure_logger(DatasetManager)
configure_logger(RequestManager)
configure_logger(FileManager)
configure_logger(Datastore)
