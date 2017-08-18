import time
from structlog import get_logger

import cassandra.cluster
import cassandra.auth
from cassandra.query import  BatchStatement

from multiprocessing import BoundedSemaphore
from data.cassandra_wrapper.model import FIELDS_Quote, FIELDS_Trades, FIELDS_Trades_min
from common import singleton, get_config, process_singleton

MAX_PARALLEL_QUERIES = 256

QUOTE_SAMPLINGS = ('raw', 'sec', 'sec_shift', 'min', 'hr')
MAX_BATCH_SIZE = 1000
_query_parallel_sema = BoundedSemaphore(MAX_PARALLEL_QUERIES)

_cassandra_enabled = True


@process_singleton
def get_cassandra_session():
    global _cassandra_enabled

    config = get_config()

    hostname = config.get('cassandra', 'hostname')
    username = config.get('cassandra', 'username')
    password = config.get('cassandra', 'password')
    keyspace = config.get('cassandra', 'keyspace')

    try:
        auth_provider = cassandra.auth.PlainTextAuthProvider(username, password)
        cluster = cassandra.cluster.Cluster([hostname], auth_provider=auth_provider)

        return cluster.connect(keyspace)
    except Exception as ex:
        get_logger().warn('could not connect to Cassandra; saving is DISABLED', ex=ex)
        _cassandra_enabled = False


@singleton
def get_async_manager():
    return AsyncManager()


def is_cassandra_enabled():
    return _cassandra_enabled


def await_all_pending_tasks():
    if not _cassandra_enabled:
        return

    while True:
        count = _query_parallel_sema.get_value()
        if count == MAX_PARALLEL_QUERIES:
            break
        get_logger().info('waiting for pending tasks', pending_count=MAX_PARALLEL_QUERIES - count)
        time.sleep(1)


def cassandra_clean_shutdown():
    if not _cassandra_enabled:
        return

    await_all_pending_tasks()

    get_cassandra_session().cluster.shutdown()


class AsyncManager:

    def __init__(self):
        pass

    def execute_async(self, session, *args, **kwargs):
        """
        :type session: cassandra.cluster.Session
        :rtype: cassandra.cluster.ResponseFuture
        """
        if not _cassandra_enabled:
            return

        _query_parallel_sema.acquire()

        future = session.execute_async(*args, **kwargs)
        future.add_callback(self._handle_success)
        future.add_errback(self._handle_failure)

        return future

    def _handle_success(self, *args):
        _query_parallel_sema.release()

    def _handle_failure(self, ex):
        get_logger().error('query failure', message=ex.message)
        _query_parallel_sema.release()


class CassQuoteRepository:

    def __init__(self, session=None):
        """
        :type session: cassandra.cluster.Session
        """
        self._session = session or get_cassandra_session()

    # __getstate__ and __setstate__ allow pickling

    def __getstate__(self):
        return ()

    def __setstate__(self, state):
        self.__init__()

    def save_async(self, quotes):
        """
        :type entry_type: str
        :type quote: RTQuote
        """

        query = \
        """
        INSERT INTO quote
        ({})
        VALUES ({})
        """.format(','.join(FIELDS_Quote),
                   ','.join("%s" for _ in FIELDS_Quote))
        batch_statement = BatchStatement()
        for quote in quotes:
            data = (quote[field] for field in FIELDS_Quote)
            batch_statement.add(query,data)
            if len(batch_statement)>= MAX_BATCH_SIZE:
                get_async_manager().execute_async(self._session,batch_statement)
                batch_statement = BatchStatement()
        if len(batch_statement) > 0:
            get_async_manager().execute_async(self._session,batch_statement)


    def load_data_async(self, market_id, selection_id, row_factory = None, fetch_size = None):

        query = \
        """
        SELECT *
        FROM quote
        WHERE market_id = '{}' and selection_id = {}
        """.format(market_id, str(selection_id))

        if row_factory is not None:
            self._session.row_factory = row_factory
        if fetch_size is not None:
            self._session.default_fetch_size = fetch_size

        result = get_async_manager().execute_async(self._session, query)

        return result

class CassTradesRepository:

        def __init__(self, session=None):
            """
            :type session: cassandra.cluster.Session
            """
            self._session = session or get_cassandra_session()

        # __getstate__ and __setstate__ allow pickling

        def __getstate__(self):
            return ()

        def __setstate__(self, trades):
            self.__init__()

        def save_async(self, trades):
            """
            :type entry_type: str
            :type quote: RTQuote
            """

            query = \
                """
                INSERT INTO trades_min
                ({})
                VALUES ({})
                """.format(','.join(FIELDS_Trades),
                           ','.join("%s" for _ in FIELDS_Quote))
            batch_statement = BatchStatement()
            for trade in trades:
                data = (trade[field] for field in FIELDS_Trades_min)
                batch_statement.add(query, data)
                if len(batch_statement) >= MAX_BATCH_SIZE:
                    get_async_manager().execute_async(self._session, batch_statement)
                    batch_statement = BatchStatement()
            if len(batch_statement) > 0:
                get_async_manager().execute_async(self._session, batch_statement)

        def load_data_async(self, market_id, selection_id, row_factory=None, fetch_size=None):

            query = \
                """
                SELECT *
                FROM tradesmin
                WHERE market_id = '{}' and selection_id = {}
                """.format(market_id, str(selection_id))

            if row_factory is not None:
                self._session.row_factory = row_factory
            if fetch_size is not None:
                self._session.default_fetch_size = fetch_size

            result = get_async_manager().execute_async(self._session, query)

            return result


class CassTradesHistRepository:
    def __init__(self, session=None):
        """
        :type session: cassandra.cluster.Session
        """
        self._session = session or get_cassandra_session()

    # __getstate__ and __setstate__ allow pickling

    def __getstate__(self):
        return ()

    def __setstate__(self, state):
        self.__init__()

    def save_async(self, trades):
        """
        :type entry_type: str
        :type quote: RTQuote
        """

        query = \
        """
        INSERT INTO trades
        ({})
        VALUES ({})
        """.format(','.join(FIELDS_Trades),
                   ','.join("%s" for _ in FIELDS_Trades))
        batch_statement = BatchStatement()
        for trade in trades:
            data = (trade[field] for field in FIELDS_Trades)
            batch_statement.add(query,data)
            if len(batch_statement)>= MAX_BATCH_SIZE:
                get_async_manager().execute_async(self._session,batch_statement)
                batch_statement = BatchStatement()
        if len(batch_statement) > 0:
            get_async_manager().execute_async(self._session,batch_statement)

    def load_data_async(self, event_id, selection_id, row_factory=None, fetch_size=None):

        query = \
            """
            SELECT *
            FROM trades
            WHERE event_id = '{}' and selection_id = {}
            """.format(event_id, str(selection_id))

        if row_factory is not None:
            self._session.row_factory = row_factory
        if fetch_size is not None:
            self._session.default_fetch_size = fetch_size

        result = get_async_manager().execute_async(self._session, query)

        return result

    def load_data_events_async(self, event_id, row_factory=None, fetch_size=None):

        query = \
            """
            SELECT *
            FROM trades
            WHERE event_id = '{}'
            """.format(event_id)

        if row_factory is not None:
            self._session.row_factory = row_factory
        if fetch_size is not None:
            self._session.default_fetch_size = fetch_size

        result = get_async_manager().execute_async(self._session, query)

        return result


    def get_all_events(self):

        query = \
            """
            SELECT DISTINCT event_id
            FROM trades
            """

        result = get_async_manager().execute_async(self._session, query)
        return result