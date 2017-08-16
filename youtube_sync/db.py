import json
import re

import youtube_dl.version

from youtube_sync.converter import (
    __converters__
)

from youtube_sync.schema import (
    Base,
    Config,
    Entity,
    Source,
    Video,
)

from youtube_dl import (
    DownloadError,
    YoutubeDL
)

from datetime import (
    datetime,
    timedelta,
)

from sqlalchemy import (
    create_engine,
)

from sqlalchemy.exc import (
    IntegrityError,
)

from sqlalchemy.orm import (
    lazyload,
    sessionmaker,
)


class Database:
    __version__ = 2

    def __init__(self, path):
        engine = create_engine(path)
        Base.metadata.create_all(engine)
        session = sessionmaker()
        session.configure(bind=engine)
        self.session = session()
        self.ydl = YoutubeDL({
            'outtmpl': u'%(extractor)s/%(uploader)s/%(title)s-%(id)s.%(ext)s',
            'match_filter': self.__match_filter,
            'extract_flat': 'in_playlist',
            'logtostderr': True,
            'usenetrc': True,
        })
        version = self.__select_config('version').first()
        if not version:
            self.session.merge(Config(
                id='version',
                value=Database.__version__
            ))
            self.session.commit()
        elif version.value == "1":
            engine.execute(
                'ALTER TABLE %s ADD COLUMN %s %s' % (
                    Source.__tablename__,
                    Source.url.name,
                    Source.url.type.compile(dialect=engine.dialect)
                )
            )
            engine.execute(
                'ALTER TABLE %s ADD COLUMN %s %s' % (
                    Source.__tablename__,
                    Source.extractor_match.name,
                    Source.extractor_match.type.compile(dialect=engine.dialect)
                )
            )
            for source in self.__query_sources():
                source.url = __converters__.get(source.extractor_key).output(source.extractor_data)
                extractor = self.__url_extractor(source.url)
                source.extractor_key = extractor.ie_key()
            for video in self.__query_videos():
                video.extractor_key = self.__name_extractor(video.extractor_key).ie_key()
            self.session.merge(Config(
                id='version',
                value=2
            ))
            self.session.commit()
        ydl_version = self.__select_config('ydl_version').first()
        if ydl_version is None or ydl_version.value != youtube_dl.version.__version__:
            for source in self.__query_sources():
                source.extractor_match = self.__url_extractor_match(source.url)
            self.session.merge(Config(
                id='ydl_version',
                value=youtube_dl.version.__version__
            ))
            self.session.commit()

    def add(self, url, delta):
        try:
            if self.__query_source(url).count():
                return False
            info = self.ydl.extract_info(url, download=False)
            self.__create_source(info, delta)
            self.session.commit()
            return True
        except IntegrityError:
            self.session.rollback()
            return False
        except DownloadError:
            self.session.rollback()
            return False

    def remove(self, url):
        source = self.__query_source(url).first()
        if not source:
            return False
        for video in source.videos:
            if self.__select_video_sources(video.id).count() == 1:
                self.session.delete(video)
        self.session.delete(source)
        self.session.commit()
        return True

    def sources(self, url=None):
        return self.__query_source(url) if url else self.__query_sources()

    def videos(self, url=None):
        return self.__query_video(url) if url else self.__query_videos()

    def fetch(self, url=None, force=False):
        for source in (self.__query_source(url) if url else self.__query_sources()).all():
            self.__update_source(source=source, download=False, force=force)

    def sync(self, url=None, force=False):
        for source in (self.__query_source(url) if url else self.__query_sources()).all():
            self.__update_source(source=source, download=True, force=force)

    def enable(self, url):
        return self.__toggle_source(url, True)

    def disable(self, url):
        return self.__toggle_source(url, False)

    def __select_config(self, key):
        return self.session.query(Config).filter(Config.id == key)

    def __query_source(self, url):
        extractor = self.__url_extractor(url)
        return self.__query_sources().\
            filter(Entity.extractor_key == extractor.ie_key()).\
            filter(Source.extractor_match == self.__url_extractor_match(url))

    def __query_video(self, url):
        info = self.ydl.extract_info(url, download=False)
        return self.__query_videos().\
            filter(Entity.extractor_key == Database.__info_extractor_key(info)).\
            filter(Entity.extractor_data == Database.__info_extractor_data(info))

    def __query_sources(self):
        return self.session.query(Source).options(lazyload('videos'))

    def __query_videos(self):
        return self.session.query(Video).options(lazyload('sources'))

    def __select_source(self, info):
        return self.__query_sources().\
            filter(Entity.extractor_key == Database.__info_extractor_key(info)).\
            filter(Entity.extractor_data == Database.__info_extractor_data(info))

    def __select_video(self, info):
        return self.__query_videos().\
            filter(Entity.extractor_key == Database.__info_extractor_key(info)).\
            filter(Entity.extractor_data == Database.__info_extractor_data(info))

    def __select_video_sources(self, id):
        return self.__query_sources().\
            filter(Source.videos.any(Video.id == id))

    def __toggle_source(self, url, allow):
        sources = self.__query_source(url).all()
        if not len(sources):
            return False
        for source in sources:
            source.allow = allow
        self.session.commit()
        return True

    def __update_source(self, source, download, force):
        if source.allow and (force or source.next <= datetime.today()):
            try:
                info = self.ydl.extract_info(source.url, download=False)
                entries = info['entries'] if 'entries' in info else [info]
                for entry_info in entries:
                    source.videos.append(self.__info_video(entry_info))
                self.session.commit()
                if download:
                    for entry_info in entries:
                        self.__download_video(entry_info)
                    source.prev = datetime.now()
                    source.next = source.prev + source.delta
                    self.session.commit()
                return True
            except DownloadError:
                self.session.rollback()
                return False

    def __download_video(self, info):
        video = self.__info_video(info)
        if video.allow and video.prev is None:
            self.ydl.process_ie_result(info)
            video.prev = datetime.now()
            self.session.commit()

    def __create_source(self, info, delta):
        source = Source(
            extractor_key=Database.__info_extractor_key(info),
            extractor_data=Database.__info_extractor_data(info),
            extractor_match=self.__info_extractor_match(info),
            url=info['webpage_url'],
            delta=delta
        )
        self.session.add(source)
        return source

    def __create_video(self, info):
        video = Video(
            extractor_key=Database.__info_extractor_key(info),
            extractor_data=Database.__info_extractor_data(info),
        )
        self.session.add(video)
        return video

    def __info_video(self, info):
        video = self.__select_video(info).first()
        if not video:
            video = self.__create_video(info)
        return video

    def __match_filter(self, info):
        video = self.__info_video(info)
        if video.prev:
            return "[sync] Video Already Downloaded: %s %s" % (
                video.extractor_key,
                video.extractor_data
            )
        if not video.allow:
            return "[sync] Video Currently Disabled: %s %s" % (
                video.extractor_key,
                video.extractor_data
            )
        return None

    @staticmethod
    def __info_extractor_key(info):
        return info['ie_key'] if 'ie_key' in info else info['extractor_key']

    @staticmethod
    def __info_extractor_data(info):
        return info['id']

    def __info_extractor_match(self, info):
        return Database.__extractor_match(
            info['webpage_url'],
            self.ydl.get_info_extractor(self.__info_extractor_key(info))
        )

    def __url_extractor_match(self, url):
        return Database.__extractor_match(url, self.__url_extractor(url))

    def __url_extractor(self, url):
        for extractor in self.ydl._ies:
            if extractor.suitable(url):
                return extractor
        return None

    def __name_extractor(self, name):
        for extractor in self.ydl._ies:
            if name == extractor.IE_NAME:
                return extractor
        return None

    @staticmethod
    def __extractor_match(url, extractor):
        return json.dumps(re.match(extractor._VALID_URL, url).groups())