import os
import logging
from enum import Enum

from geoquery.geoquery import GeoQuery
from geoquery.task import TaskList

MESSAGE_SEPARATOR = os.environ["MESSAGE_SEPARATOR"]


class MessageType(Enum):
    QUERY = "query"
    WORKFLOW = "workflow"


class Message:
    _LOG = logging.getLogger("geokube.Message")

    request_id: int
    dataset_id: str = "<unknown>"
    product_id: str = "<unknown>"
    type: MessageType
    content: GeoQuery | TaskList

    def __init__(self, load: bytes) -> None:
        self.request_id, msg_type, *query = load.decode().split(
            MESSAGE_SEPARATOR
        )
        match MessageType(msg_type):
            case MessageType.QUERY:
                self._LOG.debug("processing content of `query` type")
                assert len(query) == 3, "improper content for query message"
                self.dataset_id, self.product_id, self.content = query
                self.content: GeoQuery = GeoQuery.parse(self.content)
                self.type = MessageType.QUERY
            case MessageType.WORKFLOW:
                self._LOG.debug("processing content of `workflow` type")
                assert len(query) == 1, "improper content for workflow message"
                self.content: TaskList = TaskList.parse(query[0])
                self.dataset_id = self.content.dataset_id
                self.product_id = self.content.product_id
                self.type = MessageType.WORKFLOW
            case _:
                self._LOG.error("type `%s` is not supported", msg_type)
                raise ValueError(f"type `{msg_type}` is not supported!")
