import re
import pandas as pd
import logging
from parsers.parser import Parser
from storages.file_storage import FileStorage
from lxml import html

logger = logging.getLogger(__name__)


class SbParser(Parser):
    def parse(self, storage):
        df = pd.DataFrame([], columns=self.fields_set)

        for chunk_number, line in enumerate(storage.read_data()):
            data = line.split('\t')
            df = df.append(
                self._process_data(
                    data[1],
                    chunk_number),
                sort=False)
        return df.drop_duplicates(['href'])

    def _process_data(self, data, chunk_number):
        root = html.fromstring(data)
        elements = root.xpath("*//table")[4]
        df = pd.DataFrame([], columns=self.fields_set)

        for idx, element in enumerate(elements):
            record = {}
            try:
                if 'name' in self.fields_set:
                    name = str(element.xpath(
                        './/table/tr')[1][0][0].xpath('.//a[@class="sailheader"]/text()')[0]).strip()
                    record.update({'name': name})
                if 'href' in self.fields_set:
                    href = str(element.xpath(
                        './/table/tr')[10].xpath('.//span[@class="details"]/a/@href')[0]).strip()
                    record.update({'href': href})

                if 'advertise_date' in self.fields_set:
                    advertise_date = str(self._parse_advertiser_field(element.xpath(
                        './/table/tr')[10].xpath('.//span[@class="details"]/text()')[0])).strip()
                    record.update({'advertise_date': advertise_date})

                if 'price' in self.fields_set:
                    price = self._parse_price_field(
                        element.xpath('.//table/tr')[9].xpath('.//span[@class="sailvk"]')[0].text)
                    record.update({'price': price})

                if 'location' in self.fields_set:
                    location = str(element.xpath(
                        './/table/tr')[8].xpath('.//span[@class="sailvk"]')[0].text).strip()
                    record.update({'location': location})

                if 'hull' in self.fields_set:
                    hull = str(self._parse_hull_field(element.xpath(
                        './/table/tr')[6].xpath('.//span[@class="sailvk"]')[0].text)).strip()
                    record.update({'hull': hull})

                if 'year' in self.fields_set:
                    year = self._parse_year_field(
                        element.xpath('.//table/tr')[4].xpath('.//span[@class="sailvk"]')[0].text)
                    record.update({'year': year})

                if 'length' in self.fields_set:
                    length = self._parse_length_field(
                        element.xpath('.//table/tr')[3][1][0].text)
                    record.update({'length': length})
            except BaseException:
                logger.info(
                    f'A parsing error at the line: #{chunk_number * 10 + idx}')
            else:
                if self._is_all_fields_have_value(record):
                    df = df.append(pd.DataFrame([record]), sort=False)
        return df

    def _parse_advertiser_field(self, text):
        matcher = re.compile(r'\d\d-.{3}-\d{4}')
        return matcher.search(text).group()

    def _parse_price_field(self, text):
        return re.sub(r'\D', '', text)

    def _parse_hull_field(self, text):
        return re.sub(r'\s', ' ', text)

    def _parse_year_field(self, text):
        year = int(text.strip())
        if year < 100:
            year = year + 1900
        if year < 1900:
            return ''
        return year

    def _parse_length_field(self, text):
        return int(re.sub(r'\D', '', text))

    def _is_all_fields_have_value(self, record):
        return all([len(str(value)) > 0 for (key, value) in record.items()])
