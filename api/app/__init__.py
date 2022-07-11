from .components.access import AccessManager
from .components.dataset import DatasetManager
from .components.file import FileManager
from .components.request import RequestManager

AccessManager.configure_logger()
RequestManager.configure_logger()
DatasetManager.configure_logger()
FileManager.configure_logger()
