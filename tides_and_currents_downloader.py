#!/usr/bin/python3
import argparse
from datetime import date
from datetime import timedelta
import urllib.request
import json
import sys

from typing import Tuple, List, Generator


def first_day_of_month(d: date) -> date:
    """Return the first day of a month"""
    return date(year=d.year, month=d.month, day=1)


def last_day_of_month(d: date) -> date:
    """Return the last day of a month"""
    if d.month != 12:
        return date(year=d.year, month=d.month+1, day=1) - timedelta(days=1)
    else:
        return date(year=d.year, month=12, day=31)


def get_date_sequence(begin_date: date, end_date: date) -> List[date]:
    """Given a start and end date, return a sequence of dates spanning that time with no range being longer than 31 days."""
    date_delta = end_date - begin_date
    dates = []

    if date_delta.days <= 31:
        #print(f"{begin_date} to {end_date}")
        dates.append((begin_date, end_date))
    else:
        #print("Composite download across the following dates:")
        # Last day of begin_date's month
        ldm_begin_date = last_day_of_month(begin_date)
        #print(f"{begin_date} to {ldm_begin_date}")
        dates.append((begin_date, ldm_begin_date))

        tmp_date_start = ldm_begin_date + timedelta(days=1)
        while last_day_of_month(tmp_date_start) < end_date:
            tmp_date_end = last_day_of_month(tmp_date_start)
            #print(f"{tmp_date_start} to {tmp_date_end}")
            dates.append((tmp_date_start, tmp_date_end))
            tmp_date_start = tmp_date_end + timedelta(days=1)

        # First day of end_date's month
        fdm_end_date = first_day_of_month(end_date)
        #print(f"{fdm_end_date} to {end_date}")
        dates.append((fdm_end_date, end_date))

    return dates


def make_urls(station: str, product: str, dates: List[Tuple[date, date]], output: str) -> List[str]:
    """Given a range of dates, return the URLs spanning those dates."""
    urls = []
    template = 'https://tidesandcurrents.noaa.gov/api/datagetter?product={}&application=NOS.COOPS.TAC.MET&station={}&time_zone=GMT&units=english&interval=6&format={}&begin_date={}&end_date={}&datum=MLLW'
    for begin_date, end_date in dates:
        urls.append(template.format(product, station, output, begin_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')))

    return urls


def request_data(urls: List[str], result_type: str, show_progress = False) -> Generator[str, None, None]:
    """Given a list of URLs, return a generator that yields each line of results and skips headers after the first series."""

    if show_progress:
        print("Downloading data...", file=sys.stderr)

    for ix, url in enumerate(urls):
        if show_progress:
            print(f"{ix+1}/{len(urls)}: {url}", file=sys.stderr)

        request = urllib.request.urlopen(url)

        if result_type == 'csv':
            if ix == 0:
                lines = request.readlines()
            else:
                lines = request.readlines()[1:]

            for line in lines:
                yield line.decode()

        elif result_type == 'json':
            data = json.loads(''.join([line.decode() for line in request.readlines()]))
            if ix == 0:
                yield '{"metadata": ' + json.dumps(data['metadata']) + ',\n"data": [\n'
            for i,entry in enumerate(data['data']):
                if i < len(data['data']) - 1 or ix < len(urls) - 1:
                    yield json.dumps(entry) + ',\n'
                else:
                    yield json.dumps(entry) + '\n'

    if result_type == 'json':
        yield ']}\n'

    if show_progress:
        print("Done!", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download multiple months from NOAA's Tides & Currents API")
    parser.add_argument('station', type=str, help='Station ID (e.g. 8775870)')
    parser.add_argument('product', type=str, help='Data product (e.g. water_level)')
    parser.add_argument('begin_date', type=str, help='Date to start from (ISO 8601)')
    parser.add_argument('end_date', type=str, help='Date to end at (ISO 8601)')
    parser.add_argument('output', type=str, help='Either "csv" or "json"')

    args = parser.parse_args(sys.argv[1:])

    dates = get_date_sequence(date.fromisoformat(args.begin_date), date.fromisoformat(args.end_date))
    urls = make_urls(args.station, args.product, dates, args.output)
    data = request_data(urls, args.output, show_progress=True)
    for entry in data:
        print(entry, end='')
