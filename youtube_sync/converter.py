#!/usr/bin/env python3

import re

from youtube_dl.extractor import (
    DailymotionUserIE,
    DailymotionIE,
    VimeoIE,
    VimeoChannelIE, VimeoUserIE)

from youtube_dl.extractor.youtube import (
    YoutubeIE,
    YoutubeChannelIE,
    YoutubePlaylistIE,
    YoutubeUserIE)

from youtube_dl.utils import (
    ExtractorError)


class ConverterError(ExtractorError):
    def __init__(self, msg):
        super(ConverterError, self).__init__(msg)


class Converter:
    def __init__(self, regex, parse, template):
        self.regex = regex
        self.parse = parse
        self.template = template

    def input(self, url):
        mobj = re.match(self.regex, url)
        if mobj is None:
            raise ConverterError('Invalid URL: %s' % url)
        value = self.parse(mobj)
        if value is None:
            raise ConverterError('Invalid Id: %s' % url)
        return value

    def output(self, value):
        return self.template(value)


"""
We define a set of converters that roughly follow the unique portions of the extractors in youtube_dl.

A lot of this is shamelessly coupled with the current definitions of some of these info extractors,
so it might be better to either merge or start copying some of these regexes to avoid runtime breakage.

This type of offline analysis is quite a powerful feature and would be trivial to merge upstream
"""
__converters__ = {
    YoutubeIE.IE_NAME: Converter(
        regex=YoutubeIE._VALID_URL,
        parse=lambda mobj: mobj.group(2),
        template=lambda video_id: 'https://www.youtube.com/watch?v=%s' % video_id,
    ),
    YoutubePlaylistIE.IE_NAME: Converter(
        regex=YoutubePlaylistIE._VALID_URL,
        parse=lambda mobj: mobj.group(1) if mobj.group(1) else mobj.group(2),
        template=lambda playlist_id: YoutubePlaylistIE._TEMPLATE_URL % playlist_id,
    ),
    YoutubeChannelIE.IE_NAME: Converter(
        regex=YoutubeChannelIE._VALID_URL,
        parse=lambda mobj: mobj.group(1),
        template=lambda channel_id: YoutubeChannelIE._TEMPLATE_URL % channel_id,
    ),
    YoutubeUserIE.IE_NAME: Converter(
        # TODO: This needs a separate info extractor, it's not 1:1 anymore,
        # (@see https://github.com/rg3/youtube-dl/issues/10126)
        regex=YoutubeUserIE._VALID_URL,
        parse=lambda mobj: mobj.group(2) if mobj.group(1) == 'user' else None,
        template=lambda user_id: YoutubeUserIE._TEMPLATE_URL % ('user', user_id),
    ),
    DailymotionIE.IE_NAME: Converter(
        regex=DailymotionIE._VALID_URL,
        parse=lambda mobj: mobj.group('id'),
        template=lambda video_id: 'https://www.dailymotion.com/video/%s' % video_id
    ),
    DailymotionUserIE.IE_NAME: Converter(
        regex=DailymotionUserIE._VALID_URL,
        parse=lambda mobj: mobj.group('user'),
        template=lambda user: 'https://www.dailymotion.com/user/%s' % user
    ),
    VimeoIE.IE_NAME: Converter(
        regex=VimeoIE._VALID_URL,
        parse=lambda mobj: mobj.group('id'),
        template=lambda video_id: 'https://vimeo.com/%s' % video_id
    ),
    VimeoChannelIE.IE_NAME: Converter(
        regex=VimeoChannelIE._VALID_URL,
        parse=lambda mobj: mobj.group('id'),
        template=lambda channel_id: 'https://vimeo.com/channels/%s' % channel_id
    ),
    VimeoUserIE.IE_NAME: Converter(
        regex=VimeoUserIE._VALID_URL,
        parse=lambda mobj: mobj.group('name'),
        template=lambda name: 'https://vimeo.com/%s/videos' % name
    ),
}
