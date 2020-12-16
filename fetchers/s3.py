import logging
from pathlib import Path

import pydantic

import boto3
import boto3.resources
import botocore
import botocore.client


logger = logging.getLogger(__name__)


#class S3ApiBucket(pydantic.BaseModel):
class S3ApiBucket:
    bucket_name: str = None
    _s3 = None

    def __init__(self, bucket_name: str = None):
        self.bucket_name = bucket_name

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = boto3.resource('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))
        return self._s3

    @property
    def bucket(self):
        return self.s3.Bucket(self.bucket_name)

    def filter(self, **kwargs: dict):
        logger.debug(f"{self.bucket_name} : filtering {kwargs} ...")
        return self.bucket.objects.filter(**kwargs)

    def download(self, object_key: str, destination_dir: str, destination_filename: str = None) -> Path:

        if destination_filename is None:
            destination_filename = object_key
        fp = Path(destination_dir, destination_filename)
        if not fp.parent.exists():
            fp.parent.mkdir(parents=True)

        logger.info(f"{self.bucket_name} : downloading {object_key} to {fp} ...")
        self.bucket.download_file(object_key, str(fp))
        return fp


class NoaaGfsS3(S3ApiBucket):
    bucket_name = "noaa-gfs-bdp-pds"

    def __init__(self, **kwargs):
        pass

    def get_daterun_prefix(self, date_day: str, run: str):
        run = str(run).zfill(2)
        return f"gfs.{date_day}/{run}/gfs.t{run}z.pgrb2.0p25"

    def get_timestep_key(self, date_day: str, run: str, timestep: str):
        run = str(run).zfill(2)
        timestep = str(timestep).zfill(3)
        return f"gfs.{date_day}/{run}/gfs.t{run}z.pgrb2.0p25.f{timestep}"


def test_filter():
    s3api = NoaaGfsS3()
    r = s3api.filter(
        Prefix=s3api.get_daterun_prefix("20201216", "00")
    ).limit(count=1)
    assert isinstance(list(r), list)


def test_download():
    s3api = NoaaGfsS3()
    r = s3api.download(
        object_key=s3api.get_timestep_key("20201216", "00", "003"),
        destination_dir="/tmp/")
    print(r)
    assert isinstance(r, Path)
