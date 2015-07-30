import re

from datetime import (
    datetime,
    timedelta,
)

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    Interval,
    ForeignKey,
    String,
    Table,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.exc import (
    IntegrityError,
)

from sqlalchemy.orm import (
    lazyload,
    relationship,
    sessionmaker,
)
from sqlalchemy.ext.declarative import (
    declarative_base,
)

from youtube_dl import (
    YoutubeDL,
    gen_extractors,
)

from youtube_dl.extractor.youtube import (
    YoutubeIE,
    YoutubeChannelIE,
    YoutubeFavouritesIE,
    YoutubeHistoryIE,
    YoutubePlaylistIE,
    YoutubeRecommendedIE,
    YoutubeSearchDateIE,
    YoutubeSearchIE,
    YoutubeSearchURLIE,
    YoutubeShowIE,
    YoutubeSubscriptionsIE,
    YoutubeTruncatedIDIE,
    YoutubeTruncatedURLIE,
    YoutubeUserIE,
    YoutubeWatchLaterIE,
)

from youtube_dl.utils import (
    ExtractorError,
)

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

Base = declarative_base()


class Entity(Base):
    __tablename__ = 'entity'
    id = Column(Integer, primary_key=True, nullable=False)
    type = Column(String, nullable=False)
    prev = Column(DateTime, nullable=True)
    extractor_key = Column(String, nullable=False)
    extractor_data = Column(String, nullable=False)
    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': __tablename__,
        'with_polymorphic': '*'
    }
    __table_args__ = (
        UniqueConstraint('extractor_key', 'extractor_data', 'type', name='_entity_extractor_type'),
    )


class Video(Entity):
    __tablename__ = 'video'
    id = Column(Integer, ForeignKey(Entity.__tablename__+'.id', onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
    __mapper_args__ = {'polymorphic_identity': __tablename__}


class Source(Entity):
    __tablename__ = 'source'
    id = Column(Integer, ForeignKey(Entity.__tablename__+'.id', onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
    next = Column(DateTime, nullable=False, default=datetime.min)
    delta = Column(Interval, nullable=False)
    videos = relationship('Video', secondary=Table(
        'content', Base.metadata,
        Column('source_id', Integer, ForeignKey(__tablename__+'.id', onupdate="CASCADE", ondelete="CASCADE")),
        Column('video_id', Integer, ForeignKey(Video.__tablename__+'.id', onupdate="CASCADE", ondelete="CASCADE")),
    ))
    __mapper_args__ = {'polymorphic_identity': __tablename__}


class Config(Base):
    __tablename__ = 'config'
    id = Column(String, primary_key=True)
    value = Column(String)


class Database:
    __version__ = 1

    def __init__(self, path, log, echo=False):
        engine = create_engine(path, echo=echo)
        Base.metadata.create_all(engine)
        session = sessionmaker()
        session.configure(bind=engine)
        self.session = session()
        self.extractors = gen_extractors()
        self.converters = gen_converters()
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

    def input(self, url):
        converter = self.__create_converter(self.__create_extractor(url))
        return converter.input(url)

    def output(self, url):
        converter = self.__create_converter(self.__create_extractor(url))
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

    def sources(self):
        items = []
        for source in self.__select_sources().all():
            items.append(self.converters.get(source.extractor_key).output(source.extractor_data))
        return items

    def videos(self):
        items = []
        for video in self.__select_videos().all():
            items.append(self.converters.get(video.extractor_key).output(video.extractor_data))
        return items

    def sync(self, ydl_opts, url=None, fetch=True, download=True):
        if url is None:
            query = self.__select_sources()
        else:
            query = self.__select_source(url)
        sources = query.all()

        if url and not len(sources):
            return False

        ydl = Database.__create_ydl(ydl_opts)
        for source in sources:
            if fetch and source.next <= datetime.today():
                converter = self.converters.get(source.extractor_key)
                self.__refresh_source(ydl, source, converter.output(source.extractor_data))
            if download:
                self.__download_source(ydl, source)
        return True

    def get(self, ydl_opts, url):
        source = self.__create_source(url, timedelta(days=1))
        ydl = Database.__create_ydl(ydl_opts)
        converter = self.converters.get(source.extractor_key)
        self.__refresh_source(ydl, source, converter.output(source.extractor_data))
        self.__download_source(ydl, source)

    @staticmethod
    def __create_ydl(ydl_opts):
        return YoutubeDL(ydl_opts)

    def __create_source(self, url, delta):
        extractor = self.__create_extractor(url)
        return Source(
            extractor_key=extractor.IE_NAME,
            extractor_data=self.__create_converter(extractor).input(url),
            delta=delta,
        )

    def __create_extractor(self, url):
        for extractor in self.extractors:
            if extractor.suitable(url):
                return extractor
        return None

    def __create_converter(self, extractor):
        converter = self.converters.get(extractor.IE_NAME, None)
        if converter is None:
            raise ConverterError('Invalid IE: %s' % extractor.IE_NAME)
        return converter

    def __convert_url(self, extractor_key, extractor_data):
        return self.converters.get(extractor_key).output(extractor_data)

    def __select_config(self, key):
        return self.session.query(Config).filter(Config.id == key)

    def __select_sources(self):
        return self.session.query(Source).options(lazyload('videos'))

    def __select_videos(self):
        return self.session.query(Video)

    def __select_source(self, url):
        extractor = self.__create_extractor(url)
        return self.session.query(Source).\
            filter(Entity.extractor_key == extractor.IE_NAME).\
            filter(Entity.extractor_data == self.__create_converter(extractor).input(url))

    def __select_video(self, extractor_key, extractor_data):
        return self.__select_videos().\
            filter(Entity.extractor_key == extractor_key).\
            filter(Entity.extractor_data == extractor_data)

    def __refresh_source(self, ydl, source, url):
        for item in self.__extract_info(ydl, url, download=False):
            video = self.__create_video(ydl, item)
            if video:
                source.videos.append(video)
            else:
                self.log('Warning: Parsing error - ' + url)
        source.prev = datetime.now()
        source.next = source.prev + source.delta
        self.session.commit()

    def __download_source(self, ydl, source):
        for video in source.videos:
            if video.prev is None:
                url = self.__convert_url(video.extractor_key, video.extractor_data)
                self.__extract_info(ydl, url, download=True)
                video.prev = datetime.now()
                self.session.commit()

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
            regex=YoutubeUserIE._VALID_URL,
            parse=lambda mobj: mobj.group(1),
            template=lambda user_id: YoutubeUserIE._TEMPLATE_URL % user_id,
        ),
    }
