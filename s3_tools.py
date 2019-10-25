import pandas as pd
import boto3

#read credentials file (.csv)
#return tuple: (access_key_id, secret_access_key)
def get_credentials(credentials_path="credentials.csv", user_name=None):
    credentials = pd.read_csv(credentials_path)
    if user_name is None:
        if(credentials.shape[0] >= 1):
            return (credentials.loc[0, "access_key_id"], credentials.loc[0, "secret_access_key"]
        else:
            raise Exception("credentials file is empty")
    else:
        if(user_name in credentials["user_name"].values):
            idx = (credentials["user_name"].values==user_name).argmax()
            return (credentials.loc[idx, "access_key_id"], credentials.loc[idx, "secret_access_key"]
        else:
            raise Exception("user name <"+user_name+"> not found in credentials file")

#download all files from an S3 bucket
#destination folder must exist prior to calling this
def download_all(bucket_name, destination = "", credentials_path="credentials.csv", user_name=None, verbose=True):
    credentials = get_credentials(credentials_path, user_name)
    session = boto3.Session(aws_access_key_id = credentials[0], aws_secret_access_key = credentials[1])
    client = session.client('s3')
    ls=client.list_objects(Bucket=bucket_name)['Contents']
    for obj in ls:
        client.download_file(bucket_name, obj['Key'], destination+obj['Key'])
        if(verbose):
            print('Downloaded '+obj['Key'])
