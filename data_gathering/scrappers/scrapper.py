import logging
import requests
import sys
import re

logger = logging.getLogger(__name__)


class Scrapper(object):
    def __init__(self, skip_objects=None):
        self.skip_objects = skip_objects

    def scrap_process(self, storage):
        # You can iterate over ids, or get list of objects
        # from any API, or iterate throught pages of any site
        # Do not forget to skip already gathered data
        # Here is an example for you
        NUMBER_OF_ITEMS = 5000
        NUMBER_OF_BLOCKS = int(NUMBER_OF_ITEMS / 50)
        BASE_URL = 'https://www.sailboatlistings.com/cgi-bin/saildata/db.cgi?db=default&uid=default&view_records=1&ID=*&sb=date&so=descend&nh='
        loaded_urls = storage.get_loaded_urls(storage)

        for i in range(1, NUMBER_OF_BLOCKS + 1):
            url_to_load = BASE_URL + str(i)
            if url_to_load not in loaded_urls:
                logger.info(f'downloads block #{i} from #{NUMBER_OF_BLOCKS}')
                self._download_page(url_to_load, storage, 0)
            else:
                logger.info(f'skip cached block #{i} from #{NUMBER_OF_BLOCKS}')

    def _download_page(self, url, storage, counter):
        response = requests.get(url)
        if not response.ok:
            logger.error(response.text)
            if counter == 2:
                sys.exit()
            self._download_page(url, storage, counter + 1)
        else:
            # Note: here json can be used as response.json
            data = response.text

            # save scrapped objects here
            storage.append_data(
                [url + '\t' + re.sub('\r?\n', '', data).replace('\t', '')])
