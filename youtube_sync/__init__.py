#!/usr/bin/env python2.7

import re

from datetime import (
    datetime,
    timedelta)

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    Interval,
    ForeignKey,
    String,
    Table,
    UniqueConstraint,
    create_engine)

from sqlalchemy.exc import (
    IntegrityError)

from sqlalchemy.orm import (
    lazyload,
    relationship,
    sessionmaker)

from sqlalchemy.ext.declarative import (
    declarative_base)

from sqlalchemy.ext.hybrid import (
    hybrid_method)

from youtube_dl import (
    DownloadError,
    YoutubeDL,
    gen_extractors)

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


def gen_converters():
    """
    We define a set of converters that roughly follow the unique portions of the extractors in youtube_dl.

    A lot of this is shamelessly coupled with the current definitions of some of these info extractors,
    so it might be better to either merge or start copying some of these regexes to avoid runtime breakage.

    This type of offline analysis is quite a powerful feature and would be trivial to merge upstream
    """
    return {
        'youtube': Converter(
            regex=YoutubeIE._VALID_URL,
            parse=lambda mobj: mobj.group(2),
            template=lambda video_id: 'https://www.youtube.com/watch?v=%s' % video_id,
        ),
        'youtube:playlist': Converter(
            regex=YoutubePlaylistIE._VALID_URL,
            parse=lambda mobj: mobj.group(1) if mobj.group(1) else mobj.group(2),
            template=lambda playlist_id: YoutubePlaylistIE._TEMPLATE_URL % playlist_id,
        ),
        'youtube:channel': Converter(
            regex=YoutubeChannelIE._VALID_URL,
            parse=lambda mobj: mobj.group(1),
            template=lambda channel_id: YoutubeChannelIE._TEMPLATE_URL % channel_id,
        ),
        'youtube:user': Converter(
            # TODO: This needs a separate info extractor, it's not 1:1 anymore,
            # (@see https://github.com/rg3/youtube-dl/issues/10126)
            regex=YoutubeUserIE._VALID_URL,
            parse=lambda mobj: mobj.group(2) if mobj.group(1) == 'user' else None,
            template=lambda user_id: YoutubeUserIE._TEMPLATE_URL % ('user', user_id),
        ),
        'dailymotion': Converter(
            regex=DailymotionIE._VALID_URL,
            parse=lambda mobj: mobj.group('id'),
            template=lambda video_id: 'https://www.dailymotion.com/video/%s' % video_id
        ),
        'dailymotion:user': Converter(
            regex=DailymotionUserIE._VALID_URL,
            parse=lambda mobj: mobj.group('user'),
            template=lambda user: 'https://www.dailymotion.com/user/%s' % user
        ),
        'vimeo': Converter(
            regex=VimeoIE._VALID_URL,
            parse=lambda mobj: mobj.group('id'),
            template=lambda video_id: 'https://vimeo.com/%s' % video_id
        ),
        'vimeo:channel': Converter(
            regex=VimeoChannelIE._VALID_URL,
            parse=lambda mobj: mobj.group('id'),
            template=lambda channel_id: 'https://vimeo.com/channels/%s' % channel_id
        ),
        'vimeo:user': Converter(
            regex=VimeoUserIE._VALID_URL,
            parse=lambda mobj: mobj.group('name'),
            template=lambda name: 'https://vimeo.com/%s/videos' % name
        ),
    }


Base = declarative_base()


class Entity(Base):
    __tablename__ = 'entity'
    id = Column(Integer, primary_key=True, nullable=False)
    type = Column(String, nullable=False)
    prev = Column(DateTime, nullable=True)
    extractor_key = Column(String, nullable=False)
    extractor_data = Column(String, nullable=False)
    allow = Column(Boolean, nullable=False, default=True)
    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': __tablename__,
        'with_polymorphic': '*'
    }
    __table_args__ = (
        UniqueConstraint('extractor_key', 'extractor_data', 'type', name='_entity_extractor_type'),
    )

    @hybrid_method
    def url(self):
        return Database.converters.get(self.extractor_key).output(self.extractor_data)


Sources_to_Videos = Table(
    'content', Base.metadata,
    Column('source_id', Integer, ForeignKey('source.id', onupdate="CASCADE", ondelete="CASCADE")),
    Column('video_id', Integer, ForeignKey('video.id', onupdate="CASCADE", ondelete="CASCADE")),
)


class Video(Entity):
    __tablename__ = 'video'
    id = Column(Integer, ForeignKey(Entity.__tablename__+'.id', onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
    sources = relationship('Source', secondary=Sources_to_Videos, back_populates='videos')
    __mapper_args__ = {'polymorphic_identity': __tablename__}


class Source(Entity):
    __tablename__ = 'source'
    id = Column(Integer, ForeignKey(Entity.__tablename__+'.id', onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
    next = Column(DateTime, nullable=False, default=datetime.min)
    delta = Column(Interval, nullable=False)
    videos = relationship('Video', secondary=Sources_to_Videos, back_populates='sources')
    __mapper_args__ = {'polymorphic_identity': __tablename__}

    @hybrid_method
    def videos_missing(self):
        return len([video for video in self.videos if video.prev is None])

    @hybrid_method
    def videos_saved(self):
        return len([video for video in self.videos if video.prev is not None])

    @hybrid_method
    def videos_total(self):
        return len(self.videos)


class Config(Base):
    __tablename__ = 'config'
    id = Column(String, primary_key=True)
    value = Column(String)


class Database:
    __version__ = 1
    extractors = gen_extractors()
    converters = gen_converters()

    def __init__(self, path, log, echo=False):
        engine = create_engine(path, echo=echo)
        Base.metadata.create_all(engine)
        session = sessionmaker()
        session.configure(bind=engine)
        self.session = session()
        self.log = log

        version = self.__select_config('version').first()
        if version is None or version < Database.__version__:
            self.session.merge(Config(
                id='version',
                value=Database.__version__
            ))
            self.session.commit()

    @staticmethod
    def query(url, ydl_opts):
        """ Perform an online query without affecting persistence """
        return Database.__extract_info(Database.__create_ydl(ydl_opts), url)

    @staticmethod
    def input(url):
        converter = Database.__converter(Database.__extractor(url))
        return converter.input(url)

    @staticmethod
    def output(url):
        converter = Database.__converter(Database.__extractor(url))
        return converter.output(converter.input(url))

    def add(self, url, delta):
        try:
            self.session.add(self.__create_source(url, delta))
            self.session.commit()
            return True
        except IntegrityError:
            self.session.rollback()
            return False

    def remove(self, url):
        source = self.__select_source(url).first()
        if not source:
            return False
        self.session.delete(source)
        self.session.commit()
        return True

    def sources(self, url=None):
        return self.__query_source(url) if url else self.__query_sources()

    def videos(self, url=None):
        return self.__query_video(url) if url else self.__query_videos()

    def sync(self, ydl_opts, url=None, update=True, download=True, force=False):
        query = self.__query_source(url) if url else self.__query_sources()
        sources = query.all()

        if url and not len(sources):
            return False

        ydl = Database.__create_ydl(ydl_opts)
        if force or update:
            for source in sources:
                if source.allow and (force or source.next <= datetime.today()):
                    converter = self.converters.get(source.extractor_key)
                    self.__refresh_source(ydl, source, converter.output(source.extractor_data))
        if download:
            if url is None:
                videos = self.__query_videos().filter(Video.prev.is_(None)).filter(Video.allow.is_(True)).all()
            else:
                videos = []
                for source in sources:
                    for video in source.videos:
                        if video.prev is None and video.allow is True:
                            videos.append(video)
            for video in videos:
                self.__download_video(ydl, video)
        return True

    def get(self, ydl_opts, url):
        ydl = Database.__create_ydl(ydl_opts)
        source = self.__refresh_url(ydl, url)
        self.__download_source(ydl, source)

    def queue(self, ydl_opts, url):
        ydl = Database.__create_ydl(ydl_opts)
        self.__refresh_url(ydl, url)

    def enable(self, url):
        return self.__toggle_url(url, True)

    def disable(self, url):
        return self.__toggle_url(url, False)

    def purge(self, url):
        source = self.__select_source(url).first()
        if not source:
            return False
        for video in source.videos:
            if self.__select_video_sources(video.id).count() == 1:
                self.session.delete(video)
        self.session.delete(source)
        self.session.commit()
        return True

    @staticmethod
    def __create_ydl(ydl_opts):
        return YoutubeDL(ydl_opts)

    def __create_source(self, url, delta):
        extractor = Database.__extractor(url)
        return Source(
            extractor_key=extractor.IE_NAME,
            extractor_data=Database.__converter(extractor).input(url),
            delta=delta,
        )

    @staticmethod
    def __extractor(url):
        for extractor in Database.extractors:
            if extractor.suitable(url):
                return extractor
        return None

    @staticmethod
    def __converter(extractor):
        converter = Database.converters.get(extractor.IE_NAME, None)
        if converter is None:
            raise ConverterError('Invalid IE: %s' % extractor.IE_NAME)
        return converter

    @staticmethod
    def __entity_url(entity):
        return

    def __select_config(self, key):
        return self.session.query(Config).filter(Config.id == key)

    def __query_source(self, url):
        extractor = Database.__extractor(url)
        return self.__query_sources().\
            filter(Entity.extractor_key == extractor.IE_NAME).\
            filter(Entity.extractor_data == Database.__converter(extractor).input(url))

    def __query_video(self, url):
        extractor = Database.__extractor(url)
        return self.session.query(Video).\
            filter(Entity.extractor_key == extractor.IE_NAME).\
            filter(Entity.extractor_data == Database.__converter(extractor).input(url)).\
            options(lazyload('sources'))

    def __query_sources(self):
        return self.session.query(Source).\
            options(lazyload('videos'))

    def __query_videos(self):
        return self.session.query(Video).options(lazyload('sources'))

    def __select_entity(self, url):
        extractor = Database.__extractor(url)
        return self.session.query(Entity).\
            filter(Entity.extractor_key == extractor.IE_NAME).\
            filter(Entity.extractor_data == Database.__converter(extractor).input(url))

    def __select_source(self, url):
        extractor = Database.__extractor(url)
        return self.session.query(Source).\
            filter(Entity.extractor_key == extractor.IE_NAME).\
            filter(Entity.extractor_data == Database.__converter(extractor).input(url))

    def __select_video(self, extractor_key, extractor_data):
        return self.__query_videos().\
            filter(Entity.extractor_key == extractor_key).\
            filter(Entity.extractor_data == extractor_data)

    def __select_video_sources(self, id):
        return self.__query_sources().\
            filter(Source.videos.any(Video.id == id))

    def __toggle_url(self, url, allow):
        entities = self.__select_entity(url).all()
        '''At most there will be an entity of each type here (This is okay because hides the N-M mapping)'''
        if not len(entities):
            return False
        for entity in entities:
            entity.allow = allow
            self.session.commit()
        return True

    def __refresh_url(self, ydl, url):
        source = self.__create_source(url, timedelta(days=1))
        converter = Database.converters.get(source.extractor_key)
        self.__refresh_source(ydl, source, converter.output(source.extractor_data))
        return source

    def __refresh_source(self, ydl, source, url):
        self.log.debug('[sync] ' + url + ' : Refreshing videos')
        try:
            for item in self.__extract_info(ydl, url, download=False):
                video = self.__create_video(ydl, item)
                if video:
                    source.videos.append(video)
                else:
                    self.log.warning('[sync] ' + url + ' : Video missing extractor data')
            source.prev = datetime.now()
            source.next = source.prev + source.delta
            self.session.commit()
        except DownloadError:
            self.log.warning('[sync] ' + url + ' : Could not refresh videos')

    def __download_source(self, ydl, source):
        for video in source.videos:
            if video.prev is None and video.allow is True:
                self.__download_video(ydl, video)

    def __download_video(self, ydl, video):
        url = video.url()
        self.log.debug('[sync] ' + url + ' : Downloading video')
        try:
            self.__extract_info(ydl, url, download=True)
            video.prev = datetime.now()
            self.session.commit()
        except DownloadError:
            self.log.warning('[sync] ' + url + ' : Could not download video')

    def __create_video(self, ydl, item):
        ''' YoutubeDL basically returns a pile of context-sensitive garbage identifying things that it *might* be '''
        ''' (...because using variant-type key-value APIs didn't destroy enough life on earth during the 80's!) '''
        extractor_key = self.__extract_key(ydl, item)
        if extractor_key is None:
            return None
        extractor_data = item['id']
        video = self.__select_video(extractor_key, extractor_data).first()
        if video is None:
            video = Video(
                extractor_key=extractor_key,
                extractor_data=extractor_data,
            )
            self.session.add(video)
            self.session.commit()
        return video

    @staticmethod
    def __extract_key(ydl, item):
        ie_key = item.get('ie_key', None)
        if ie_key:
            ''' Unextracted content seems to return an internal ie key '''
            return ydl.get_info_extractor(ie_key).IE_NAME
        else:
            ''' Extracted content seems to use the public ie name '''
            return item.get('extractor', None)

    @staticmethod
    def __extract_info(ydl, url, download=False):
        info = ydl.extract_info(url, download=download)
        return info['entries'] if 'entries' in info else [info]

