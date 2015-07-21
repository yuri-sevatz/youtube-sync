import re
import youtube_dl

from sqlalchemy import Column
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy import Boolean, String, Integer, DateTime, Interval
from sqlalchemy import Table
from sqlalchemy import create_engine

from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from datetime import datetime, timedelta

Base = declarative_base()

class Filter:
    def __init__(self, pattern, replace):
        self.pattern = pattern
        self.replace = replace

    def convert(self, value):
        if re.search(self.pattern, value):
            if self.replace:
                return re.sub(self.pattern, self.replace, value)
            else:
                return value
        return None


class Converter:
    def __init__(self, input_filter, output_filter):
        self.input_filter = input_filter
        self.output_filter = output_filter

    def input(self, value):
        return self.input_filter.convert(value)

    def output(self, value):
        return self.output_filter.convert(value)

    def migrate(self, value):
        return self.input(self.output(value))


class Entity(Base):
    __tablename__ = 'entity'
    id = Column(Integer, primary_key=True, nullable=False)
    type = Column(String, nullable=False)
    prev = Column(DateTime, nullable=True)
    extractor_id = Column(String, nullable=False)
    extractor_data = Column(String, nullable=False)
    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': __tablename__,
        'with_polymorphic': '*'
    }
    __table_args__ = (
        UniqueConstraint('extractor_id', 'extractor_data', 'type', name='_entity_extractor_type'),
    )

    def upgrade(self, converter):
        self.extractor_id = converter.id
        self.extractor_data = converter.migrate(self.extractor_data)

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
        self.extractors = youtube_dl.gen_extractors()
        self.converters = converters()
        self.log = log

        version = self.__select_config('version').first()
        if version is None or version < Database.__version__:
            self.session.merge(Config(
                id='version',
                value=Database.__version__
            ))

    def upgrade(self):
        """ Upgrade Entities using new Extractors """
        for entity in self.session.query(Entity).all():
            converter = self.converters.get(entity.extractor_id)
            if converter is not None:
                entity.upgrade(converter)
                self.session.merge(entity)

    def query(self, url, ydl_opts):
        """ Perform an online query without affecting persistence """
        ydl = youtube_dl.YoutubeDL(ydl_opts)
        info = self.__extract_info(ydl, url)
        return info['entries'] if 'entries' in info else [info]

    def insert(self, url, delta):
        source = self.__create_source(url, delta)
        if source:
            self.session.add(source)
            return True
        else:
            return False

    def update(self, url, ydl_opts):
        """ Perform an online query that affects persistence """
        source = self.__select_source(url).first()
        return source and self.__update_source(youtube_dl.YoutubeDL(ydl_opts), source, url)

    def delete(self, url):
        source = self.__select_source(url).first()
        if not source:
            return False
        return self.session.delete(source)

    def input(self, url):
        extractor = self.__create_extractor(url)
        return self.__convert_input(url, extractor) if extractor else None

    def output(self, url):
        extractor = self.__create_extractor(url)
        return self.__convert_output(url, extractor) if extractor else None

    def sources(self):
        items = []
        for source in self.__select_sources().all():
            converter = self.converters.get(source.extractor_id)
            items.append(source.extractor_data if converter is None else converter.output(source.extractor_data))
        return items

    def sync(self, ydl_opts):
        ydl = youtube_dl.YoutubeDL(ydl_opts)
        for source in self.__select_sources().filter(Source.next <= datetime.now()).all():
            converter = self.converters.get(source.extractor_id)
            url = source.extractor_data if converter is None else converter.output(source.extractor_data)
            self.__update_source(ydl, source, url)

    def __create_source(self, url, delta):
        extractor = self.__create_extractor(url)
        return None if not extractor else Source(
            extractor_id=extractor.IE_NAME,
            extractor_data=self.__convert_input(url, extractor),
            delta=delta,
        )

    def __create_extractor(self, url):
        for extractor in self.extractors:
            if extractor.suitable(url):
                return extractor
        return None

    def __convert_output(self, url, extractor):
        parser = self.converters.get(extractor.IE_NAME)
        return parser.output(url) if parser else url

    def __convert_input(self, url, extractor):
        parser = self.converters.get(extractor.IE_NAME)
        return parser.input(url) if parser else url

    def __select_config(self, key):
        return self.session.query(Config).filter(Config.id == key)

    def __select_sources(self):
        return self.session.query(Source)

    def __select_source(self, url):
        extractor = self.__create_extractor(url)
        if not extractor:
            return None
        return self.session.query(Source).\
            filter(Entity.extractor_id == extractor.IE_NAME).\
            filter(Entity.extractor_data == self.__convert_input(url, extractor))

    def __select_video(self, extractor_id, extractor_data):
        return self.session.query(Video).\
            filter(Entity.type == Video.__tablename__).\
            filter(Entity.extractor_id == extractor_id).\
            filter(Entity.extractor_data == extractor_data)

    def __update_source(self, ydl, source, url):
        info = self.__extract_info(ydl, url)
        if not info:
            return False
        if 'entries' in info:
            for item in info['entries']:
                if not self.__update_video(ydl, source, item):
                    self.log('Warning: Parsing error - ' + url)
        else:
            self.__update_video(ydl, source, info)
        source.prev = datetime.now()
        source.next = source.prev + source.delta
        return True

    def __update_video(self, ydl, source, item):
        ''' YoutubeDL basically returns a pile of context-sensitive garbage identifying things that it *might* be '''
        ''' (...because using variant-type key-value APIs didn't destroy enough life on earth during the 80's!) '''
        extractor_key = self.__extract_key(ydl, item)
        if extractor_key is None:
            return False
        extractor_id = extractor_key
        extractor_data = item['id']
        video = self.__select_video(extractor_id, extractor_data).first()
        if video is None:
            video = Video(
                extractor_id=extractor_id,
                extractor_data=extractor_data,
            )
        source.videos.append(video)
        return True

    @staticmethod
    def __extract_key(ydl, item):
        ie_key = item.get('ie_key', None)
        if ie_key:
            ''' Unextracted content seems to return an internal ie key '''
            return ydl.get_info_extractor(ie_key)
        else:
            ''' Extracted content seems to use the public ie name '''
            return item.get('extractor', None)

    @staticmethod
    def __extract_info(ydl, url):
        return ydl.extract_info(url, download=False, process=True)


def converters():
    '''
    We define a set of default extractors that roughly follow the unique portions of the extractors in youtube_dl.
    (These would be GREAT to merge upstream, because they let us extract a unique identity offline, using any url!)

    Extractors that can't be matched with one of these don't maintain uniqueness, but the output filter code is
    strong enough to support re-parsing uris at a later time.
    '''
    return {
        'youtube': Converter(
            input_filter=Filter(
                pattern='^(?:https?://)?(?:(?:www|m)\.)?youtube\.[a-z]{2,3}/watch\?v=([^&#]+).*$',
                replace='\\1'
            ),
            output_filter=Filter(
                pattern='^(?:(?:https?://)?(?:(?:www|m)\.)?youtube\.[a-z]{2,3}/watch\?v=)?([^&#]+)',
                replace='www.youtube.com/watch?v=\\1'
            )
        ),
        'youtube:playlist': Converter(
            input_filter=Filter(
                pattern='^(?:https?://)?(?:(?:www|m)\.)?youtube\.[a-z]{2,3}/playlist\?list=([^&#]+).*$',
                replace='\\1'
            ),
            output_filter=Filter(
                pattern='^(?:(?:https?://)?(?:(?:www|m)\.)?youtube\.[a-z]{2,3}/playlist\?list=)?([^&#]+)',
                replace='www.youtube.com/playlist?list=\\1'
            )
        ),
        'youtube:channel': Converter(
            input_filter=Filter(
                pattern='^(?:https?://)?(?:(?:www|m)\.)?youtube\.[a-z]{2,3}/channel/([^/\?&]+).*$',
                replace='\\1'
            ),
            output_filter=Filter(
                pattern='^(?:(?:https?://)?(?:(?:www|m)\.)?youtube\.[a-z]{2,3}/channel/)?([^/\?&]+)',
                replace='www.youtube.com/channel/\\1'
            )
        ),
        'youtube:user': Converter(
            input_filter=Filter(
                pattern='^(?:https?://)?(?:(?:www|m)\.)?youtube\.[a-z]{2,3}/user/([^/\?&]+).*$',
                replace='\\1'
            ),
            output_filter=Filter(
                pattern='^(?:(?:https?://)?(?:(?:www|m)\.)?youtube\.[a-z]{2,3}/user/)?([^/\?&]+)',
                replace='www.youtube.com/user/\\1'
            )
        ),
    }
