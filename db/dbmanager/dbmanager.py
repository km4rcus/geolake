from __future__ import annotations

from datetime import datetime
from enum import auto, Enum as Enum_, unique

from sqlalchemy import (
    Column,
    create_engine,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    JSON,
    Sequence,
    String
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker


@unique
class RequestStatus(Enum_):
    PENDING = auto()
    RUNNING = auto()
    DONE = auto()
    FAILED = auto()


class _Repr:
    def __repr__(self):
        cols = self.__table__.columns.keys()  # pylint: disable=no-member
        kwa = ', '.join(f'{col}={getattr(self, col)}' for col in cols)
        return f'{type(self).__name__}({kwa})'


Base = declarative_base(cls=_Repr, name='Base')


class Role(Base):
    __tablename__ = 'roles'
    role_id = Column(Integer, Sequence('role_id_seq'), primary_key=True)
    role_name = Column(String(255), nullable=False, unique=True)


class User(Base):
    __tablename__ = 'users'
    user_id = Column(Integer, primary_key=True)
    keycloak_id = Column(Integer, nullable=False, unique=True)
    api_key = Column(String(255), nullable=False, unique=True)
    contact_name = Column(String(255))
    role_id = Column(Integer, ForeignKey('roles.role_id'))


class Worker(Base):
    __tablename__ = 'workers'
    worker_id = Column(Integer, primary_key=True)
    status = Column(String(255), nullable=False)
    host = Column(String(255))
    dask_scheduler_port = Column(Integer)
    dask_dashboard_address = Column(String(10))
    created_on = Column(DateTime, nullable=False)


class Request(Base):
    __tablename__ = 'requests'
    request_id = Column(Integer, primary_key=True)
    status = Column(Enum(RequestStatus), nullable=False)
    priority = Column(Integer)
    user_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    worker_id = Column(Integer, ForeignKey('workers.worker_id'))
    dataset = Column(String(255))
    product = Column(String(255))
    query = Column(JSON())
    estimate_bytes_size = Column(Integer)
    download_id = Column(Integer, unique=True)
    created_on = Column(DateTime, nullable=False)
    last_update = Column(DateTime)


class Download(Base):
    __tablename__ = 'downloads'
    download_id = Column(
        Integer, primary_key=True
    )
    download_uri = Column(String(255))
    storage_id = Column(Integer)
    location_path = Column(String(255))
    bytes_size = Column(Integer)
    created_on = Column(DateTime, nullable=False)


class Storage(Base):
    __tablename__ = 'storages'
    storage_id = Column(Integer, primary_key=True)
    name = Column(String(255))
    host = Column(String(20))
    protocol = Column(String(10))
    port = Column(Integer)


class DBManager:
    def __init__(
        self,
        database: str = 'dds',
        host: str = 'db',
        port: int = 5432,
        user: str = 'dds',
        password: str = 'dds'
    ) -> None:
        url = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        self.__engine = engine = create_engine(url, echo=True)
        self.__session_maker = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)

    def create_request(
        self,
        user_id: int = 1,
        dataset: str | None = None,
        product: str | None = None,
        query: str | None = None,
        worker_id: int | None = None,
        priority: str | None = None,
        estimate_bytes_size: int | None = None,
        download_id: int | None = None,
        status: RequestStatus = RequestStatus.PENDING,
    ) -> int:
        # TODO: Add more request-related parameters to this method.
        with self.__session_maker() as session:
            request = Request(
                status=status,
                priority=priority,
                user_id=user_id,
                worker_id=worker_id,
                dataset=dataset,
                product=product,
                query=query,
                estimate_bytes_size=estimate_bytes_size,
                download_id=download_id,
                created_on=datetime.utcnow()
            )
            session.add(request)
            session.commit()
            return request.request_id

    def update_request(
        self,
        request_id: int,
        worker_id: int,
        status: RequestStatus
    ) -> int:
        with self.__session_maker() as session:
            request = session.query(Request).get(request_id)
            request.status = status
            request.worker_id = worker_id
            request.last_update = datetime.utcnow()
            session.commit()
            return request.request_id

    def get_request_status(
        self,
        request_id
    ) -> RequestStatus:
        with self.__session_maker() as session:
            request = session.query(Request).get(request_id)
            return request.status

    def create_worker(
        self,
        status: str,
        dask_scheduler_port: int,
        dask_dashboard_address: int,
        host: str = 'localhost'
    ) -> int:
        with self.__session_maker() as session:
            worker = Worker(
                status=status,
                host=host,
                dask_scheduler_port=dask_scheduler_port,
                dask_dashboard_address=dask_dashboard_address,
                created_on=datetime.utcnow()
            )
            session.add(worker)
            session.commit()
            return worker.worker_id