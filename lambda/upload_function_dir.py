import os
import zipfile
import boto3


def upload_s3(path, bucket='practice-pal'):
    """
    file will automatically be uploaded to 'lambda-funcs' dir within bucket
    :param path: path to file needed to be uploaded
    :param bucket: s3 bucket
    :return:
    """
    s3 = boto3.client('s3')
    s3_path = 'lambda-funcs/' + os.path.basename(path)
    s3.upload_file(Filename=path, Bucket=bucket, Key=s3_path)


def zipdir(path, file_name):
    """
    Zip a directory
    :param path: path where files to zip are held
    :param file_name: name of zip file
    :return:
    """
    with zipfile.ZipFile(file_name, 'w') as zip_handle:
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.find('.zip') < 0:
                    zip_handle.write(os.path.join(root, file))


def upload_function(s3_path, function_name, bucket='practice-pal'):
    """
    Upload a zip file containing lambda function using boto3
    :param s3_path: full path within bucket to zip file with lambda code
    :param function_name: str: lambda function name
    :param bucket: str: name of s3 bucket
    :return: read docs of boto3.client('lambda').update_function_code()
    """
    lambda_resource = boto3.client('lambda')
    response = lambda_resource.update_function_code(
        FunctionName=function_name,
        S3Bucket=bucket,
        S3Key=s3_path
    )
    return response


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('functionName', help='name of lambda function', type=str)
    parser.add_argument('-d', '--directory', help='directory of files to be zipped',
                        default='./', type=str)
    args = parser.parse_args()

    # print(args.functionName)
    zip_name = args.functionName + '.zip'
    zip_path_full = args.directory + zip_name
    s3_zip_path = 'lambda-funcs/' + zip_name

    # take action
    zipdir(args.directory, zip_path_full)
    upload_s3(zip_path_full)

    upload_function(s3_path=s3_zip_path, function_name=args.functionName)
