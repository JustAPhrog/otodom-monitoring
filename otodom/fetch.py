from datetime import datetime
from typing import NamedTuple

from loguru import logger

from otodom.filter_parser import parse_flats_for_filter
from otodom.flat_filter import FlatFilter
from otodom.models import Flat
from otodom.storage import (
    NewAndUpdateFlats,
    StorageContext,
    dump_fetched_flats,
    dump_new_flats,
    dump_updated_flats,
    filter_new_flats,
    get_total_flats_in_db,
    insert_flats,
    update_flats,
)


class FetchedFlats(NamedTuple):
    new_flats: list[Flat]
    update_flats: list[Flat]
    total_flats: int


def fetch_and_persist_flats(
    storage_context: StorageContext, ts: datetime, flat_filter: FlatFilter
):
    filter_name = flat_filter.name
    limit_pages=1
    start_page=0
    last_page=5
    flats = parse_flats_for_filter(flat_filter, now=ts, start_page=start_page, limit_pages=limit_pages)
    result_flats = NewAndUpdateFlats([], [])
    while len(flats) != 0:
        dump_fetched_flats(flats, filter_name, storage_context, now=ts)

        logger.info("Fetched {} flats", len(flats))
        new_and_updated_flats = filter_new_flats(
            storage_context.sqlite_conn, flats, filter_name=filter_name
        )
        logger.info("Found {} new flats", len(new_and_updated_flats.new_flats))
        logger.info("Found {} updated flats", len(new_and_updated_flats.updated_flats))

        dump_new_flats(
            new_and_updated_flats.new_flats, filter_name, storage_context, now=ts
        )
        dump_updated_flats(
            new_and_updated_flats.updated_flats, filter_name, storage_context, now=ts
        )

        insert_flats(
            storage_context.sqlite_conn, new_and_updated_flats.new_flats, filter_name
        )
        update_flats(
            storage_context.sqlite_conn, new_and_updated_flats.updated_flats, filter_name
        )
        result_flats.new_flats.append(new_and_updated_flats.new_flats)
        result_flats.updated_flats.append(new_and_updated_flats.updated_flats)
        if last_page <= start_page:
            break
        start_page += 1
        flats = parse_flats_for_filter(flat_filter, now=ts, start_page=start_page, limit_pages=limit_pages)
    
    total_flats = get_total_flats_in_db(storage_context.sqlite_conn, filter_name)
    return FetchedFlats(
        new_flats=result_flats.new_flats,
        update_flats=result_flats.updated_flats,
        total_flats=total_flats,
    )
