import logging
import time
import os.path
import argparse
import multiprocessing
import json
from decimal import Decimal
from datetime import timedelta
from datetime import datetime

import requests
import panda
from progress.bar import Bar

"""
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger('requests.packages.urllib3')
logger.setLevel(logging.DEBUG)
logger.propagate = True
"""


class VideoFileLoadError(Exception):
    """
    Error raised if any of the local video file fails to be loaded.
    """


class PandaStreamUploadError(Exception):
    """
    Error raised when the upload to PandaStream fails.
    """


class VideoFile(object):
    """
    ADT for a local video file.
    """

    def __init__(self, base_dir, file_path):
        self.path = self._compute_full_path(base_dir, file_path)
        self.name = self._compute_file_name()
        self.size = self._compute_file_size()

    def __str__(self):
        return "%s (%s bytes)" % (self.path, self.size)

    def __repr__(self):
        return "<VideoFile %s>" % str(self)

    def _compute_full_path(self, base_dir, file_path):
        return os.path.abspath(os.path.join(base_dir, file_path))

    def _compute_file_name(self):
        return os.path.basename(self.path)

    def _compute_file_size(self):
        try:
            return os.path.getsize(self.path)
        except IOError, e:
            raise VideoFileLoadError(str(e))


class PandaStreamUploadSession(object):
    """
    Represents a PandaStream upload session.
    """
    def __init__(self, video_file, upload_url):
        self.video_file = video_file
        self.upload_url = upload_url

    def __str__(self):
        return self.upload_url

    def __repr__(self):
        return "<PandaStreamUploadSession '%s' -> '%s'>" % (
            self.video_file,
            self.upload_url)


class PandaStreamVideo(object):
    """
    Represents a PandaStream video resource.
    """
    def __init__(self, video_file, resource_url):
        self.video_file = video_file
        self.resource_url = resource_url
        self.progress = Decimal("0.0")

    def __str__(self):
        return self.resource_url

    def __repr__(self):
        return "<PandaStreamVideo '%s' @ '%s' (%.2f%%)>" % (
            self.video_file,
            self.resource_url,
            self.progress)


class PandaStreamEncodingProfiler(object):
    """
    Command ADT to upload a set of video files to PandaStream and measure the
    encoding time.
    """

    def __init__(self, service, video_files):
        self._service = service
        self._video_files = video_files

    def run(self):
        videos = PandaStreamUploader(self._service, self._video_files).run()

        progress_bar = Bar('Processing', max=100)
        encoder = PandaStreamEncoder(self._service, videos)
        encoding_time = encoder.run(progress_bar=progress_bar)

        print "\nEncoding took %s seconds" % encoding_time


def init_upload_session(args):
    """
    Wrapper over the `PandaStreamEncodingProfiler._init_upload_session` method
    to be used with `multiprocessing.Pool`.
    """
    return PandaStreamUploader._init_upload_session(*args)


def do_upload_video_file(args):
    """
    Wrapper over the `PandaStreamEncodingProfiler._do_upload_video_file` method
    to  the upload session for a video file.
    """
    return PandaStreamUploader._do_upload_video_file(*args)


class PandaStreamUploader(object):
    """
    Command ADT to upload a set of video files to PandaStream in parallel
    """

    def __init__(self, service, video_files, processes=None):
        self._service = service
        self._video_files = video_files
        self._process_count = processes or (multiprocessing.cpu_count() * 2)

    def run(self):
        upload_sessions = self._init_upload_sessions()
        return self._do_upload(upload_sessions)

    def _init_upload_sessions(self):
        pool = multiprocessing.Pool(processes=self._process_count)

        result = pool.map(
            init_upload_session,
            zip([self] * len(self._video_files), self._video_files),
            chunksize=5)

        upload_sessions = []
        for index, upload_url in enumerate(result):
            session = PandaStreamUploadSession(
                self._video_files[index],
                upload_url)
            upload_sessions.append(session)

        return upload_sessions

    def _init_upload_session(self, video_file):
        print "Getting upload session for '%s'" % video_file.path

        response = self._service.post("/videos/upload.json", {
            'file_name': video_file.name,
            'file_size': video_file.size,
            'use_all_profiles': True,
            'path_format': "profiling/:date/:video_id/:profile/:id",
        })
        # TODO - refactor to do json.loads in the PandaStream lib
        response = json.loads(response)
        return response['location']

    def _do_upload(self, upload_sessions):
        pool = multiprocessing.Pool(processes=self._process_count)

        result = pool.map(
            do_upload_video_file,
            zip([self] * len(upload_sessions), upload_sessions),
            chunksize=5)

        videos = []
        for index, video_id in enumerate(result):
            video = PandaStreamVideo(
                upload_sessions[index].video_file,
                self._video_resource_url(video_id))
            videos.append(video)

        return videos

    def _do_upload_video_file(self, session):
        video_file = session.video_file
        print "Uploading '%s' to '%s'" % (video_file.name, session.upload_url)

        with open(video_file.path, 'rb') as f:
            response = requests.post(session.upload_url, data=f, headers={
                'content-type': 'application/octet-stream'
            })

            try:
                response.raise_for_status()
            except IOError, e:
                raise PandaStreamUploadError(str(e))

        response_data = response.json()
        return response_data['id']

    def _video_resource_url(self, video_id):
        return '/videos/%s.json' % video_id


class PandaStreamEncoder(object):

    def __init__(self, service, videos, checking_interval=3):
        self._service = service
        self._videos = videos
        self._checking_interval = checking_interval

    def _set_start_time(self):
        self._start_time = datetime.utcnow()

    def run(self, progress_bar=None):
        self._set_start_time()

        total_progress = Decimal("0.0")

        while total_progress < Decimal("100.0"):
            current_progress = Decimal("0.0")

            for video in self._videos:
                current_progress += self._get_video_progress(video)

            current_progress /= len(self._videos)
            total_progress = current_progress

            if progress_bar:
                progress_bar.goto(int(total_progress))

            time.sleep(self._checking_interval)

        return self._get_encoding_time()

    def _get_video_progress(self, video):
        response = self._service.get(self._get_encodings_url(video))
        response_data = json.loads(response)

        transcoding_progress = Decimal("0.0")
        for encoding in response_data:
            transcoding_progress += Decimal(
                encoding['encoding_progress'] or "0.0")

        return transcoding_progress / len(response_data)

    def _get_encodings_url(self, video):
        return video.resource_url.replace(".json", "/encodings.json")

    def _get_encoding_time(self):
        delta = (datetime.utcnow() - self._start_time)
        return delta.seconds


def get_arguments():
    parser = argparse.ArgumentParser(
        description=("Upload all the provided video files to PandaStream and"
                     " measure the encoding time"))
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
        '--access-key',
        required=True,
        dest='access_key',
        action='store',
        help="The PandaStream API access key")
    parser.add_argument(
        '--secret-key',
        required=True,
        dest='secret_key',
        action='store',
        help="The PandaStream API secret key")
    parser.add_argument(
        '--cloud-id',
        required=True,
        dest='cloud_id',
        action='store',
        help="The ID of PandaStream cloud to use")

    parser.add_argument(
        '--base-dir',
        dest='base_dir',
        default='',
        action='store',
        help=("The base dir where the provided video files are located. "
              " Defaults to '' (relative paths)"))
    parser.add_argument(
        'video_files',
        metavar='video-file',
        action='store',
        nargs="+",
        help="A list of video file paths to upload")

    return _post_process_args(parser.parse_args())


def _post_process_args(args):
    args.video_files = _load_video_files(args)
    args.service = _init_pandastream_service(args)
    return args


def _load_video_files(args):
    video_files = []
    for vf in args.video_files:
        video_files.append(VideoFile(args.base_dir, vf))

    return video_files


def _init_pandastream_service(args):
    return panda.Panda(
        api_host=args.api_host,
        cloud_id=args.cloud_id,
        access_key=args.access_key,
        secret_key=args.secret_key,
        api_port=args.api_port)


def main():
    args = get_arguments()

    video_id = 'ad299f48991fa88003a37ae7fbd4d6c9'
    print args.service.get('/videos/%s.json' % video_id)
    print args.service.get('/videos/%s/encodings.json' % video_id)

    #uploader = PandaStreamEncodingProfiler(args.service, args.video_files)
    #uploader.run()


if __name__ == "__main__":
    main()
