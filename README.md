## PandaStream Toolset

A set of tools to ease the management of the PandaStream clouds.

### Bootstrap

In order to bootstrap the toolset you have to first clone the repository
and then create the virtual environment.

 ```bash
$ git clone git@github.com:roverdotcom/pandastream-tools.git
$ cd pandastream/
$ virtualenv venv/
$ source venv/bin/activate
$ pip install -r requirements.txt
```

### Synchronizing encoding profiles

The toolset exposes a simple synchronization script for encoding profiles to
PandaStream cloud of choice.

```bash
(venv)$ python sync_profiles.py -h
```

#### Profiles config file

The included profiles.cfg.example file, contains an example of different
profiles including thumbnails, MP4 and HLS. You may use this file as the
starting point in defining your profiles.
