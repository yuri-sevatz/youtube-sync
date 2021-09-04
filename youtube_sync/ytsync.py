#!/usr/bin/env python3

import youtube_dl.version

from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import lazyload, sessionmaker
from youtube_sync import db, ytdl


class YoutubeSync:
    __version__ = 2

    def __init__(self, path, params=None):
        engine = create_engine(path)
        db.Base.metadata.create_all(engine)
        session = sessionmaker()
        session.configure(bind=engine)
        self.session = session()
        if params:
            match_filter = params.get('match_filter', None)
            params['match_filter'] = lambda info: self.__match_filter(info, match_filter)
        else:
            params = {
                'match_filter': self.__match_filter
            }
        self.ytdl = ytdl.YoutubeDL(params)
        version = self.__get_config('version')
        if not version:
            self.__set_config('version', YoutubeSync.__version__)
        elif version.value == "1":
            raise Exception("Migration from V1 no longer supported")
        ydl_version = self.__get_config('ydl_version')
        if ydl_version is None or ydl_version.value != youtube_dl.version.__version__:
            for source in self.__query_sources():
                source.extractor_match = self.ytdl.get_matcher_from_url(source.url)
            self.__set_config('ydl_version', youtube_dl.version.__version__)
            self.session.commit()

    def add(self, url, delta):
        try:
            if self.__query_source(url).count():
                return False
            info = self.ytdl.extract_info(url, download=False)
            self.__create_source(info, delta)
            self.session.commit()
            return True
        except IntegrityError:
            self.session.rollback()
            return False
        except youtube_dl.DownloadError:
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

    def __get_config(self, key):
        return self.session.query(db.Config).filter(db.Config.id == key).first()

    def __set_config(self, key, value):
        self.session.merge(db.Config(
            id=key,
            value=value
        ))
        self.session.commit()

    def __query_source(self, url):
        extractor = self.ytdl.get_info_extractor_from_url(url)
        return self.__query_sources().\
            filter(db.Entity.extractor_key == extractor.ie_key()).\
            filter(db.Source.extractor_match == self.ytdl.get_matcher_from_url(url))

    def __query_video(self, url):
        info = self.ytdl.extract_info(url, download=False)
        return self.__query_videos().\
            filter(db.Entity.extractor_key == self.ytdl.get_key_from_info(info)).\
            filter(db.Entity.extractor_data == self.ytdl.get_data_from_info(info))

    def __query_sources(self):
        return self.session.query(db.Source).options(lazyload('videos'))

    def __query_videos(self):
        return self.session.query(db.Video).options(lazyload('sources'))

    def __select_source(self, info):
        return self.__query_sources().\
            filter(db.Entity.extractor_key == self.ytdl.get_key_from_info(info)).\
            filter(db.Entity.extractor_data == self.ytdl.get_data_from_info(info))

    def __select_video(self, info):
        return self.__query_videos().\
            filter(db.Entity.extractor_key == self.ytdl.get_key_from_info(info)).\
            filter(db.Entity.extractor_data == self.ytdl.get_data_from_info(info))

    def __select_video_sources(self, video_id):
        return self.__query_sources().\
            filter(db.Source.videos.any(db.Video.id == video_id))

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
                self.ytdl.to_stdout('[ytsync] Updating %s' % source.url)
                info = self.ytdl.extract_info(source.url, download=False)
                merge = self.__select_source(info).first()
                if merge and merge != source:
                    for video in source.videos:
                        merge.videos.append(video)
                    self.session.delete(source)
                    source = merge
                else:
                    source.extractor_key = self.ytdl.get_key_from_info(info)
                    source.extractor_data = self.ytdl.get_data_from_info(info)
                source.extractor_match = self.ytdl.get_matcher_from_info(info)
                entries = info['entries'] if 'entries' in info else [info]
                for entry_info in entries:
                    source.videos.append(self.__info_video(entry_info))
                self.session.commit()
                if download:
                    result = True
                    for entry_info in entries:
                        try:
                            self.__download_video(entry_info)
                        except youtube_dl.DownloadError:
                            result = False
                            self.session.rollback()
                    if result:
                        source.prev = datetime.now()
                        source.next = source.prev + source.delta
                        self.session.commit()
            except youtube_dl.DownloadError:
                self.session.rollback()

    def __download_video(self, info):
        video = self.__info_video(info)
        if video.allow and video.prev is None:
            self.ytdl.to_stdout('[ytsync] Downloading %s %s' % (video.extractor_key, video.extractor_data))
            self.ytdl.process_ie_result(info)
            video.prev = datetime.now()
            self.session.commit()

    def __create_source(self, info, delta):
        source = db.Source(
            extractor_key=self.ytdl.get_key_from_info(info),
            extractor_data=self.ytdl.get_data_from_info(info),
            extractor_match=self.ytdl.get_matcher_from_info(info),
            url=info['webpage_url'],
            delta=delta
        )
        self.session.add(source)
        return source

    def __create_video(self, info):
        video = db.Video(
            extractor_key=self.ytdl.get_key_from_info(info),
            extractor_data=self.ytdl.get_data_from_info(info),
        )
        self.session.add(video)
        return video

    def __info_video(self, info):
        video = self.__select_video(info).first()
        if not video:
            video = self.__create_video(info)
        return video

    def __match_filter(self, info, match_filter=None):
        video = self.__info_video(info)
        if video.prev:
            return "[ytsync] Video Already Downloaded: %s %s" % (
                video.extractor_key,
                video.extractor_data
            )
        if not video.allow:
            return "[ytsync] Video Currently Disabled: %s %s" % (
                video.extractor_key,
                video.extractor_data
            )
        return match_filter(info) if match_filter else None
