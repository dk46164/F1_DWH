# Databricks notebook source
def s3_mnt():
    aws_access_key_id = "{acess key id}"
    aws_secret_access_key = "{access secret key}"
    aws_bucket_name = "f1-stage"
    mount_name = "f1-s3-mnt"
    encoded_secret_key = aws_secret_access_key.replace("/", "%2F")
    try :
        dbutils.fs.mount("s3a://%s:%s@%s" % (aws_access_key_id, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
    except Exception as e:
        print(f'There is already mount point with {mount_name}')

    
    
