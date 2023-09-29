import pandas as pd
import numpy as np
from tqdm import tqdm
from flask import make_response, jsonify, request
from flask_restful import Resource,Api

REDSHIFT_DRIVER_NAME = 'redshift+redshift_connector' # indicate redshift_connector driver and dialect will be used
REDSHIFT_HOST ='staging-datawarehouse-useast1-redshift-db01.cvqbn5uvh50n.us-east-1.redshift.amazonaws.com'
REDSHIFT_PORT = 5439 # Amazon Redshift port
REDSHIFT_DATABASE = 'warehouse' # Amazon Redshift database
REDSHIFT_USERNAME = 'dwadmin' # Amazon Redshift username
REDSHIFT_PASSWORD = 'FoQvP8RV&jfgWKOG^&Y$8Pqa!' # Amazon Redshift password



JP_REDSHIFT_URL = create_engine(f"postgresql://{REDSHIFT_USERNAME}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DATABASE}")

query = '''
        SELECT * FROM "warehouse"."job_progression_path"."job_progression_20_09_23_V2"
        '''



df = pd.read_sql_query(query, JP_REDSHIFT_URL)
df.sort_values(by="percentage", ascending=False, inplace = True)
df.reset_index(inplace = True, drop = True)

class JobProgressionPathAPIV2(Resource):
    def get(self, current_user):
        logger = log.get_logger("Job progression api")
        title = request.args.get("query")
        title = " ".join(str(title).split()).title()
        try:
            logger.info("Recieved request from job progresssion api")
            if title:
                df_filtered = df[df["source"] == title]
                result_list = []
                for _, row in df_filtered.iterrows():
                    result_dict = {"Target": row["target"], "Percentage": row["percentage"], "Time_span": row["time_span"]}
                    result_list.append(result_dict)

                resp = {"status": "success", "job_progressions_path": result_list, "Title": title}
                logger.info("job progression data fetch from redshift")
                status_code = 200

            else:
                resp = {"status": "Failure",  "job_progressions_path": f"{title} not found / JOb progression path not available"}
                logger.info("Job progression data not vailable.")
                status_code = 200

        except Exception as err:
            logger.info(f"Got an error for job progression api : {err}")
            resp = {"status": "Failure",  "Reason": str(err)}

        return make_response(jsonify(resp), status_code)
