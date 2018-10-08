# youtube-sync

## Introduction

Youtube-sync is a wrapper around youtube-dl that allows for automatic synchronization of videos.

What makes it different from the average bash loop we find on the internet for mass-downloading with youtube-dl, is that youtube-sync tracks relational associations between video sources being monitored and the contained videos.  This helps it de-duplicate server requests that overlap on referenced videos.  It tracks and avoids re-requesting information from videos that it already knows about (something youtube-dl does not avoid in all cases!)

Youtube-sync does all this without writing any site-specific functionality, with the intent of seemlessly supporting most if not all of youtube-dl's supported backends.

Youtube-sync benefits from a deeper understanding of what youtube-dl considers to be "the same source" of videos.  When two URLs point to the same info extractor data, youtube-sync is quite aware of this collision.  This allows youtube-sync to avoid duplicating sources at times where the average script for automating youtube-dl might lead to multiple copies of the same user/channel.

The point of youtube-sync is to track associations to help you figure out where videos come from, and to raise the bar on what is theoretically possible to learn and extract from youtube-dl's API.

This tool is EXPERIMENTAL.  The current version has been playing mostly well with upgrades and downgrades of youtube-dl's API.  Though use at your own risk.

## Description
```
usage: ytsync [-h] [-p PATH] [-o [OUTPUT]] [-f]
              {init,add,remove,sources,videos,parents,children,status,fetch,sync,enable,disable}
              [url]

SyncDB Shell Tool

positional arguments:
  {init,add,remove,sources,videos,parents,children,status,fetch,sync,enable,disable}
                        Action
  url                   Unique Url (default: None)

optional arguments:
  -h, --help            show this help message and exit
  -p PATH, --path PATH  Database Path (default: None)
  -o [OUTPUT], --output [OUTPUT]
                        Output template (default: )
  -f, --force           Force Update (default: False)
```
## Installation

```
git clone <repo url>
cd youtube-sync
mkdir build
cmake ../
make
make install
```

## Usage

Initialize a new ytsync repository:
```
mkdir ~/ytsync
cd ~/ytsync
youtube-sync init
```

Adding a Source
```
cd ~/ytsync
youtube-sync add https://www.youtube.com/playlist?list=PL6B3937A5D230E335
youtube-sync status

Sources:
[  0  of  ?  ] https://www.youtube.com/playlist?list=PL6B3937A5D230E335

Videos:
```

Fetching Videos:
```
cd ~/ytsync
youtube-sync fetch
youtube-sync status

Sources:
[  0  of  ?  ] https://www.youtube.com/playlist?list=PL6B3937A5D230E335

Videos:
[  0  of  1  ] Youtube mN0zPOpADL4
[  0  of  1  ] Youtube SkVqJ1SGeL0
[  0  of  1  ] Youtube lqiN98z6Dak
[  0  of  1  ] Youtube Y-rmzh0PI3c
[  0  of  1  ] Youtube Z4C82eyhwgU
[  0  of  1  ] Youtube eRsGyueVLvQ
[  0  of  1  ] Youtube R6MlUcmOul8
[  0  of  1  ] Youtube YE7VzlLtp-4
[  0  of  1  ] Youtube TLkA0RELQ1g
[  0  of  1  ] Youtube aqz-KE-bpKQ

```

Downloading Videos
```
cd ~/ytsync
youtube-sync sync
youtube-sync status

Sources:
[  10  of  10  ] https://www.youtube.com/playlist?list=PL6B3937A5D230E335

Videos:
[  1  of  1  ] Youtube mN0zPOpADL4
[  1  of  1  ] Youtube SkVqJ1SGeL0
[  1  of  1  ] Youtube lqiN98z6Dak
[  1  of  1  ] Youtube Y-rmzh0PI3c
[  1  of  1  ] Youtube Z4C82eyhwgU
[  1  of  1  ] Youtube eRsGyueVLvQ
[  1  of  1  ] Youtube R6MlUcmOul8
[  1  of  1  ] Youtube YE7VzlLtp-4
[  1  of  1  ] Youtube TLkA0RELQ1g
[  1  of  1  ] Youtube aqz-KE-bpKQ

```


## Design

The database inside youtube-sync sits on top of an M to N relational mapping among sources you are syncing, and videos.  A little inheritance is used for common information, but the rest is fairly trivial information.

Deduplication among sources is made possible by comparing any [extractor_key, extractor_data] pair with youtube-dl's internal responses when preparing to fetch information from a URL.  Each pair returned for a particular URL is unique to each version of youtube-dl, but pairs are possible to generate while offline.  This makes them trivial for youtube-sync to update automatically during detected youtube-dl verison updates.  With this information, all responses from the same version of youtube-dl dealing with similar URLs that comparably point to the same web resource are now possible to identify what they are duplicates of, even if the URLs are not the same -- despite this functionality not existing anywhere in youtube-dl on the whole.

This de-duplicating provides youtube-sync with the ability to prevent sources from being added twice, and the power to query a source's synchronization status on demand using any website URL format supported by youtube-dl, without any changes required in youtube-sync's design to support each additional website added to youtube-dl.

```
CREATE TABLE config (
	id VARCHAR NOT NULL, 
	value VARCHAR, 
	PRIMARY KEY (id)
);
CREATE TABLE entity (
	id INTEGER NOT NULL, 
	type VARCHAR NOT NULL, 
	prev DATETIME, 
	extractor_key VARCHAR NOT NULL, 
	extractor_data VARCHAR NOT NULL, 
	allow BOOLEAN NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT _entity_extractor_type UNIQUE (extractor_key, extractor_data, type), 
	CHECK (allow IN (0, 1))
);
CREATE TABLE source (
	id INTEGER NOT NULL, 
	next DATETIME NOT NULL, 
	delta DATETIME NOT NULL, url VARCHAR, extractor_match VARCHAR, 
	PRIMARY KEY (id), 
	FOREIGN KEY(id) REFERENCES entity (id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE video (
	id INTEGER NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(id) REFERENCES entity (id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE content (
	source_id INTEGER, 
	video_id INTEGER, 
	FOREIGN KEY(source_id) REFERENCES source (id) ON DELETE CASCADE ON UPDATE CASCADE, 
	FOREIGN KEY(video_id) REFERENCES video (id) ON DELETE CASCADE ON UPDATE CASCADE
);

```
