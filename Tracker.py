from azure.data.tables import TableServiceClient
import datetime
import uuid

class JobTracker(object):
    def __init__(self):
        self.storage = "equitymarketdatastorage"
        self.table = "tracker"
        self.key = "DefaultEndpointsProtocol=https;AccountName=equitymarketdatastorage;AccountKey=0Pjp/4C3REg7xPeNZulrdlcm85uSgj3mtonuvHyZcxNkDtvUyDmDqaum2rDj9qxucgJgHpLfDKCstiQ3UsMo8Q==;EndpointSuffix=core.windows.net"

    def assign_job_id(self, jobname):
        job_id =  jobname + '-' + str(datetime.datetime.now().date())
        return job_id

    def update_job_status(self, jobname, status, error=None):
        job_id = self.assign_job_id(jobname)
        print("Job ID Assigned: {}".format(job_id))
        update_time = datetime.datetime.now()
        table = self.get_db_connection()
        try:
            my_entity = {
                            u'PartitionKey': jobname,
                            u'RowKey': str(uuid.uuid1()).split('-')[0],
                            u'Timestamp': update_time,
                            u'JobID': job_id,
                            u'Status': status,
                            u'Error': error
                        }
            entity = table.create_entity(entity=my_entity)
        except Exception as error:
            print("Error executing db statement for job tracker: ", error)
        return

    def get_job_status(self, job_id):
        # connect db and send sql query
        table = self.get_db_connection()
        try:
            query = "JobID eq '%s'" % (job_id)
            records = list(table.query_entities(query))
            last_record = dict(records[-1])
            last_record_status = last_record['Status']
            return last_record_status
        except Exception as error:
            print("Error getting job status: ", error)
        return

    def get_db_connection(self):
        connection = None
        try:
            connection = TableServiceClient.from_connection_string(conn_str=self.key)
            table = connection.get_table_client(table_name=self.table)
            print("Connected to storage.")
        except Exception as error:
            print("Error while connecting to storage: ", error)
        return table

class Reporter(object):
    """
    job_id, status, updated_time
    """
    def __init__(self, jobname, dbconfig):
        self.jobname = jobname
        self.dbconfig = dbconfig
    
    def report(self):
        return

# def run_reporter_etl(my_config):
#     trade_date = my_config.get('production', 'processing_date')
#     reporter = Reporter(spark, my_config)
#     tracker = Tracker('analytical_etl', my_config)
#     try:
#         reporter.report(spark, trade_date, eod_dir)
#         tracker.update_job_status("success")
#     except Exception as e:
#         print(e)
#         tracker.update_job_status("failed")
#     return