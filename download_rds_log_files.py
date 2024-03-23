# 参考)
# * https://qiita.com/takumats/items/cc0f9df02d789f896917
import boto3
from botocore.awsrequest import AWSRequest
import botocore.auth as auth
import requests
import argparse

def get_file_names_from_rds(region, instance_id):
    session = boto3.session.Session()
    rds_client = session.client('rds', region_name=region)
    response = rds_client.describe_db_log_files(DBInstanceIdentifier = instance_id)
    return [file["LogFileName"] for file in response["DescribeDBLogFiles"]]

def download_log_file_from_rds(region, instance_id, file_name):
    remote_host = 'rds.' + region + '.amazonaws.com'
    url = 'https://' + remote_host + '/v13/downloadCompleteLogFile/' + instance_id + '/' + file_name
    session = boto3.session.Session()
    credentials = session.get_credentials()
    sigv4auth = auth.SigV4Auth(credentials, 'rds', region)
    awsreq = AWSRequest(method = 'GET', url = url)
    sigv4auth.add_auth(awsreq)
    res = requests.get(url, stream=True, headers={
            'Authorization': awsreq.headers['Authorization'],
            'X-Amz-Date': awsreq.context['timestamp'],
            'X-Amz-Security-Token': credentials.token
        })
    if (res.status_code < 200 or res.status_code >= 300):
        print('http status-error : ' + str(res.status_code));
        res.close()
        return
    out_file_name = file_name.replace('/','-')
    f = open(out_file_name, mode='w', encoding='utf-8')
    for chunk in res.iter_lines():
        wstr = chunk.decode("utf-8")
        wstr += '\n';
        f.write(wstr)
    f.close()
    res.close()

def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        '-r',
        '--region',
        metavar='REGION',
        default='ap-northeast-1',
        help='リージョンを指定する（デフォルト：ap-northeast-1）'
    )
    arg_parser.add_argument(
        'instance_id',
        metavar='INSTANCE_ID',
        help='インスタンスIDを指定する'
    )

    # 引数取得
    args = arg_parser.parse_args()
    region = args.region
    instance_id = args.instance_id

    file_names = get_file_names_from_rds(region, instance_id)
    for file_name in file_names:
        print(file_name)
        download_log_file_from_rds(region, instance_id, file_name)

if __name__ == '__main__':
    main()