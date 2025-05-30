# small lib with a FlightCache and FlightStorage implementations

from attr import frozen, field
from attr.validators import instance_of
from xorq.caching import (
    Cache,
    CacheStorage,
    SnapshotStrategy,
)
import xorq as xo
from xorq.common.utils.logging_utils import get_print_logger
from xorq.flight.client import FlightClient
from xorq.vendor.ibis.expr import types as ir
from toolz import curry, pipe, compose


logger = get_print_logger()


@curry
def get_table_schema(client, key):
    result = client.do_action_one("get_schema_using_query", f"select * from '{key}'")
    return result if result else None


@curry
def create_table_expr(key, schema):
    if schema is None:
        return None
    return xo.table(schema=schema, name=key)


@curry
def execute_query(client, expr):
    if expr is None:
        return None
    return client.execute_query(expr)


@curry
def to_memtable_op(table):
    return xo.memtable(table).op()


@curry
def upload_to_flight(client, key, table):
    client.upload_data(key, table)
    return table


@frozen
class FlightStorage(CacheStorage):
    client: FlightClient = field(validator=instance_of(FlightClient))
    source: xo.vendor.ibis.backends.BaseBackend = field(
        validator=instance_of(xo.vendor.ibis.backends.BaseBackend),
        factory=xo.config._backend_init,
    )

    def key_exists(self, key):
        try:
            schema = get_table_schema(self.client, key)
            return schema is not None
        except Exception:
            logger.warning("Key does not exist.")
            return False

    def _get(self, key):
        try:
            result = pipe(
                key,
                get_table_schema(self.client),
                create_table_expr(key),
                execute_query(self.client),
                to_memtable_op,
            )

            if result is not None:
                return result

            raise KeyError(f"Key not found in Flight storage: {key}")
        except Exception as e:
            raise KeyError(f"Error retrieving from Flight storage: {key}") from e

    def _put(self, key, value):
        arrow_table = xo.to_pyarrow(value.to_expr())
        return pipe(
            arrow_table,
            lambda table: upload_to_flight(self.client, key, table),
            to_memtable_op,
        )

    def _drop(self, key):
        return NotImplementedError(f"drop is not implemented: {key}")


@frozen
class FlightCache:
    client: FlightClient = field(validator=instance_of(FlightClient))
    source: xo.vendor.ibis.backends.BaseBackend = field(
        validator=instance_of(xo.vendor.ibis.backends.BaseBackend),
        factory=xo.config._backend_init,
    )
    cache: Cache = field(init=False)

    def __attrs_post_init__(self):
        flight_cache = pipe(
            FlightStorage(client=self.client, source=self.source),
            lambda storage: Cache(strategy=SnapshotStrategy(), storage=storage),
        )

        object.__setattr__(self, "cache", flight_cache)

    def exists(self, expr: ir.Expr) -> bool:
        return self.cache.exists(expr)

    def __getattr__(self, attr):
        get_attr_from_components = compose(
            lambda obj_list: next(
                (getattr(obj, attr) for obj in obj_list if hasattr(obj, attr)), None
            ),
            lambda: [self.cache, self.cache.storage, self.cache.strategy],
        )

        result = get_attr_from_components()
        if result is not None:
            return result

        return object.__getattribute__(self, attr)
