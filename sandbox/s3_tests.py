import boto3
from botocore import UNSIGNED
from botocore.client import Config

# Sources of documentation :
#   - Tutorial from https://github.com/planet-os/notebooks/blob/master/aws/era5-s3-via-boto.ipynb
#   - Retrieving subfolders names in S3 bucket from boto3, https://stackoverflow.com/a/57718002/554374
#   - https://github.com/boto/boto3/issues/134
#   - S3 Without authentication, https://stackoverflow.com/a/34866092/554374


s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))

# See https://registry.opendata.aws/noaa-gfs-bdp-pds/
gfs_bucket = "noaa-gfs-bdp-pds"


for r in s3.Bucket(gfs_bucket).objects.limit(count=10).filter(Prefix="gfs.20201215/00"):
    print(r)


print("plop\n\n")
bucket = s3.Bucket(gfs_bucket)
# Check if run 00z is present
results = bucket.objects.limit(count=1).filter(Prefix="gfs.20201215/00/gfs.t00z.pgrb2.0p25")
print(list(results))
if len(list(results)) > 0:
    print("Check if forecastrange 003 is present")
    f003 = bucket.objects.filter(Prefix="gfs.20201215/00/gfs.t00z.pgrb2.0p25.f003")
    print(f003)
    print(len(list(f003)))

exit(0)
#s3.Bucket(gfs_bucket).objects.limit(count=10).filter(Prefix="gfs.20201216/00"): print(r)






# Use s3 without authentication
# cf. https://stackoverflow.com/a/34866092/554374
#
client = boto3.client('s3', config=Config(signature_version=UNSIGNED))



# Search
paginator = client.get_paginator('list_objects')
result = paginator.paginate(Bucket=gfs_bucket, Delimiter='/')
for prefix in result.search('CommonPrefixes'):
    print(prefix.get('Prefix'))


import q; q.d()

# Print out bucket names
#or bucket in s3.buckets.all():
#    print(bucket.name)


#"https://noaa-gfs-bdp-pds.s3.amazonaws.com/?list-type=2&delimiter=%2F&prefix=gfs.20201215%2F"