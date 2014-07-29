import logging
import json
import argparse
from ConfigParser import SafeConfigParser

import panda

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger('requests.packages.urllib3')
logger.setLevel(logging.DEBUG)
logger.propagate = True


class ServiceError(Exception):
    pass


class EncodingProfilesSynchronizer(object):

    def __init__(self, service):
        self._service = service

    def run(self, profiles):
        current_profiles = self._fetch_profiles()

        for current_profile in current_profiles:
            profile_name = current_profile['name']
            if profile_name in profiles:
                new_profile = profiles.pop(profile_name)
                self._update_profile(current_profile, new_profile)

        for new_profile in profiles.values():
            self._create_profile(new_profile)

    def _fetch_profiles(self):
        current_profiles = self._service.get('/profiles.json')
        return json.loads(current_profiles)

    def _update_profile(self, current_profile, new_profile):
        payload = current_profile.copy()
        payload.update(new_profile)
        payload.pop('preset_name')
        profile_id = payload.pop('id')

        self._service.put('/profiles/%s.json' % profile_id, payload)
        print "Updated profile '%s'" % current_profile['name']

    def _create_profile(self, new_profile):
        self._service.post('/profiles.json', new_profile)
        print "Created profile '%s'" % new_profile['name']


def get_config_parser(filename):
    config = SafeConfigParser()
    with open(filename) as config_file:
        config.readfp(config_file)

    return config


def load_profiles_from_config_parser(parser):
    profiles = {}

    for profile_name in parser.sections():
        profile = {'name': profile_name}

        for field, value in parser.items(profile_name):
            profile[field] = value

        profiles[profile_name] = profile

    return profiles


def load_profiles_from_file(filename):
    parser = get_config_parser(filename)
    return load_profiles_from_config_parser(parser)


def get_arguments():
    parser = argparse.ArgumentParser(
        description=("Synchronize the profiles in the configuration file "
                     "to the provided PandaStream cloud"))
    parser.add_argument(
        '--api-host',
        dest='api_host',
        action='store',
        default='api.pandastream.com',
        help="The PandaStream API URL (defaults to %(default)s)")
    parser.add_argument(
        '--api-port',
        dest='api_port',
        action='store',
        default='443',
        help=("The PandaStream API port to use. Possible values: 80 and 443 "
              "(defaults to %(default)s)"))
    parser.add_argument(
        'access_key',
        action='store',
        help="The PandaStream API access key")
    parser.add_argument(
        'secret_key',
        action='store',
        help="The PandaStream API secret key")
    parser.add_argument(
        'cloud_id',
        action='store',
        help="The ID of PandaStream cloud to use")
    parser.add_argument(
        '--profiles-file',
        dest='profiles_file',
        action='store',
        default='profiles.cfg',
        help=("The path to the configuration file containing the profiles to "
              "synchronize (defaults to %(default)s)"))

    return parser.parse_args()


def main():
    args = get_arguments()

    service = panda.Panda(
        api_host=args.api_host,
        cloud_id=args.cloud_id,
        access_key=args.access_key,
        secret_key=args.secret_key,
        api_port=args.api_port)

    synchronizer = EncodingProfilesSynchronizer(service)
    profiles = load_profiles_from_file(args.profiles_file)

    try:
        synchronizer.run(profiles)
    except ServiceError, e:
        print "Failed to synchronize profiles: %s" % e


if __name__ == "__main__":
    main()
