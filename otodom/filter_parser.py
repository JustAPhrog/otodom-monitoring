from datetime import datetime
from operator import attrgetter
from time import sleep

import requests as r
from loguru import logger
from toolz.itertoolz import unique

from otodom.constants import USER_AGENT
from otodom.flat_filter import FlatFilter
from otodom.listing_page_parser import OtodomFlatsPageParser
from otodom.models import Flat

PAGE_HARD_LIMIT = 100


def fetch_listing_html(url: str) -> str:
    headers = {"User-Agent": USER_AGENT}
    resp = r.get(url, headers=headers)
    resp.raise_for_status()
    return resp.text


def parse_flats_for_filter(
    filter: FlatFilter,
    now: datetime,
    sleep_for: int = 3,
    start_page: int = None,
    limit_pages: int = None
) -> list[Flat]:
    dumped_flat = False
    page_idx = start_page if start_page else 0
    flats = []
    while True:
        sleep(sleep_for)
        page_idx += 1
        url = filter.with_page(page_idx).compose_url()
        logger.info("Querying {}", url)
        html = fetch_listing_html(url)
        parser = OtodomFlatsPageParser.from_html(html, now=now)
        if parser.is_empty():
            break
        parsed_flats = parser.parse()
        dumped_flat = True
        if not parsed_flats:
            raise RuntimeError(
                "Looks like there's a next page but the parser failed to parse any flats"
            )
        flats.extend(parser.parse())
        if limit_pages and limit_pages == page_idx - start_page:
            break
    return list(unique(flats, attrgetter("url")))

def parse_flat_for_filter(
    filter: FlatFilter,
    flat_url: str,
    now: datetime,
    sleep_for: int = 3,
) -> Flat:
    sleep(sleep_for)

    logger.debug("Querying {}", flat_url)