from redshift_utils import ScriptReader
from redshift_utils import RedshiftDataManager
from settings import SCRIPT_PATH
from settings import DB_CONNECTION
import boto3


class LoadData(object):

    def loadstates(self):

        copy_command = '''copy dbapp.ustates
        from 's3://crimeanalysisyk/ustates.csv'
        iam_role 'arn:aws:iam::234001707946:role/RedshiftLab'
        delimiter ',';'''
        #script = ScriptReader.get_script(SCRIPT_PATH)
        result = RedshiftDataManager.run_update(copy_command, DB_CONNECTION)



    def loadgunviolence(self):
            copy_command = '''copy
            dbapp.gun_violence
            from
            's3://crimeanalysisyk/gun-violence.csv'
            iam_role
            'arn:aws:iam::234001707946:role/RedshiftLab'
            delimiter
            ','
            ACCEPTANYDATE
            dateformat
            'MM/DD/YYYY';'''
            result = RedshiftDataManager.run_update(copy_command, DB_CONNECTION)


    def loadhatecrimes(self):
            copy_command ='''copy
            dbapp.hatecrimes
            from
            's3://crimeanalysisyk/hate_crime.csv'
            iam_role
            'arn:aws:iam::234001707946:role/RedshiftLab'
            csv;'''
            result = RedshiftDataManager.run_update(copy_command, DB_CONNECTION)



