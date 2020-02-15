#!/usr/bin/env python3

from datetime import (
    datetime,
)

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
)

from sqlalchemy.orm import (
    relationship,
)

from sqlalchemy.ext.declarative import (
    declarative_base,
)

from sqlalchemy.ext.hybrid import (
    hybrid_method,
)


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
    url = Column(String, nullable=False)
    extractor_match = Column(String, nullable=False)
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
