from redshift_utils import Messages
from redshift_utils import ScriptReader
from redshift_utils import RedshiftDataManager
from settings import SCRIPT_PATH
from settings import DB_CONNECTION
import boto3


class Exec(object):
    @staticmethod
    def execute():

        script = ScriptReader.get_script(SCRIPT_PATH)
        r = RedshiftDataManager.run_query(script, DB_CONNECTION)

        bucket_name = "crimeanalysisyk"
        file_name = "test.txt"
        lambda_path = "/tmp/" + file_name
        s3_path = "output/" + file_name

        s3 = boto3.resource("s3")
        s3.Bucket(bucket_name).put_object(Key=s3_path, Body=(bytes(r.encode('UTF-8'))), ServerSideEncryption='AES256')
        return RedshiftDataManager.run_query(script, DB_CONNECTION)

if __name__=="__main__":
    obj = Exec()
    obj.execute
