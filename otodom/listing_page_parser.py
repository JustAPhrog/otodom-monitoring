import base64
import json
import re
from datetime import datetime
from operator import itemgetter
from urllib.parse import urljoin
import requests as r
from time import sleep

import pytz
from bs4 import BeautifulSoup
from toolz import concat, unique
from typing_extensions import Self

from otodom.models import Flat
from otodom.constants import USER_AGENT

PRICE_RE = re.compile(r"([0-9 ]+)\szł/mc")


def _make_tz_aware(dt: datetime) -> datetime:
    tz = pytz.timezone("Europe/Warsaw")
    return dt.replace(tzinfo=tz)


def get_page_html(offer_url):
    headers = {"User-Agent": USER_AGENT}
    resp = r.get(offer_url, headers=headers)
    if resp.status_code in [502]:
        return
    return resp.text

class OtodomFlatsPageParser:
    def __init__(self, soup: BeautifulSoup, now: datetime, html: str):
        self.soup = soup
        self.now = now
        self.html = html

    @classmethod
    def from_html(cls, html: str, now: datetime) -> Self:
        soup = BeautifulSoup(html, "html.parser")
        return cls(soup=soup, now=now, html=html)

    def is_empty(self) -> bool:
        return bool(self.soup.find_all(attrs={"data-cy": "no-search-results"}))

    def parse(self) -> list[Flat]:
        data = self.soup.find_all(attrs={"id": "__NEXT_DATA__"})
        if not data:
            raise RuntimeError(
                f"Failed to fetch data from from html: base64 {base64.b64encode(self.html.encode('utf8'))}"
            )
        payload: dict = json.loads(data[0].text)
        # if not dumped_flat:
        #     with open('./data/dumped_flat.json', 'w') as f:
        #         f.write(data[0].text)

        items = unique(
            concat(
                [
                    payload["props"]["pageProps"]["data"]["searchAds"]["items"],
                    payload["props"]["pageProps"]["data"]["searchAdsRandomPromoted"][
                        "items"
                    ],
                ]
            ),
            key=itemgetter("id"),
        )
        result = []
        for item in items:
            hide_price = item["hidePrice"]
            if not hide_price:
                
                offer_url = f'https://otodom.pl/pl/oferta/{item["slug"]}'
                sleep(3)
                offer_html = get_page_html(offer_url)
                if offer_html is None:
                    continue
                offer_data = BeautifulSoup(offer_html, "html.parser").find_all(attrs={"id": "__NEXT_DATA__"})

                offer_payload:dict = json.loads(offer_data[0].text)
                try:
                    offer_item = offer_payload["props"]["pageProps"]["ad"]
                    rooms = None
                    if offer_item["target"].get("Rooms_num") and len(offer_item["target"]["Rooms_num"]) > 0:
                        rooms = offer_item["target"]["Rooms_num"][0]
                    garage = 1 if offer_item["target"].get("Extras_types") and offer_item["target"]["Extras_types"].count("garage") == 1 else 0
                    if offer_item["target"].get("Build_year"):
                        build_year = offer_item["target"]["Build_year"]
                except KeyError as e:
                    print(e.args)
                    with open('./data/key_error.json', 'w') as f:
                        f.write(offer_data[0].text)
                result.append(
                    Flat(
                        url=offer_url,
                        found_ts=self.now,
                        title=item["title"],
                        area=item["areaInSquareMeters"],
                        rooms=rooms,
                        garage=garage,
                        build_year=build_year,
                        summary_location=item["locationLabel"]["value"].split(', ')[1],
                        price=item["totalPrice"]["value"],
                        created_dt=_make_tz_aware(datetime.fromisoformat(item["dateCreated"])),
                        flat_id=offer_url.split('-')[-1],
                        pushed_up_dt=datetime.fromisoformat(item["pushedUpAt"])
                        if item["pushedUpAt"]
                        else None,
                    )
                )
        return result