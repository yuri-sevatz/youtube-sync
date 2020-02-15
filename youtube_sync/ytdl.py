#!/usr/bin/env python3

import json
import re
import youtube_dl


class YoutubeDL(youtube_dl.YoutubeDL):
    """
    Wraps YoutubeDL's API with some useful convenience functions for persisting unique identities.

    YoutubeDL makes horrific use of key-value dicts for pretty much everything internally,
    and exposes pretty much nothing of its internal riches in any kind of constructive/predictable order.

    Youtube Sync makes use of some of this info to build predictability within YoutubeDL's loose API contracts.

    (We delve into some private data here, but what it is obtaining is mostly trivial and more than reasonable info)
    """
    def __init__(self, params=None, auto_init=True):
        super(YoutubeDL, self).__init__(params, auto_init)

    @staticmethod
    def get_key_from_info(info):
        """
        Get an info extractor's key from an info dict

        @type info: dict
        @param info: An info dict returned from an InfoExtractor

        @rtype: str
        @return: Returns the InfoExtractor's key from an info dict, or None otherwise
        """
        return info['ie_key'] if 'ie_key' in info else info['extractor_key']

    @staticmethod
    def get_data_from_info(info):
        """
        Get the data for a url described by the provided info dict

        @type info: dict
        @param info: An info dict returned from an InfoExtractor

        @rtype: str
        @return: Returns a unique id of a url described
        """
        return info['id']

    def get_info_extractor_from_info(self, info):
        """
        Get a suitable InfoExtractor for a given info dict, or None otherwise

        @type info: dict
        @param info: An info dict returned from an InfoExtractor

        @rtype: youtube_dl.extractor.common.InfoExtractor
        @return: Returns a suitable InfoExtractor for the provided info dict, or None otherwise
        """
        return self.get_info_extractor(self.get_key_from_info(info))

    def get_info_extractor_from_name(self, name):
        """
        Get an InfoExtractor from its internal name, or None otherwise

        @type name: str
        @param name: The IE_NAME of an InfoExtractor (Note that this is different than InfoExtractor.ie_key())

        @rtype: youtube_dl.extractor.common.InfoExtractor
        @return: Returns the infoExtractor with the provided name, or None otherwise
        """

        for extractor in self._ies:
            if extractor.IE_NAME is name:
                return extractor
        return None

    def get_info_extractor_from_url(self, url):
        """
        Get a suitable InfoExtractor for a given URL, or None otherwise

        @type url: str
        @param url: A Url

        @rtype: youtube_dl.extractor.common.InfoExtractor
        @return: Returns a suitable InfoExtractor for the provided url, or None otherwise
        """
        for extractor in self._ies:
            if extractor.suitable(url):
                return extractor
        return None

    def get_matcher_from_info(self, info):
        """
        Generates a string to uniquely identify an info dict's referenced content, within a given InfoExtractor's range

        @type info: dict
        @param info: An info dict returned from an InfoExtractor

        @rtype: str
        @return: Returns a string within the range of the specific InfoExtractor, to uniquely identify the info dict
        """
        return YoutubeDL.__create_matcher(info['webpage_url'], self.get_info_extractor_from_info(info))

    def get_matcher_from_url(self, url):
        """
        Generates a string to uniquely identify a URL's referenced content, within a given InfoExtractor's range

        @type url: str
        @param url: A Url

        @rtype: str
        @return: Returns a string within the range of the specific InfoExtractor, to uniquely identify the URL.
        """
        return YoutubeDL.__create_matcher(url, self.get_info_extractor_from_url(url))

    @staticmethod
    def __create_matcher(url, extractor):
        return json.dumps(re.match(extractor._VALID_URL, url).groups())
