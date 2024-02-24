import urllib.parse
import pandas as pd
import sqlalchemy

from sqlalchemy import create_engine, text
from dotenv import dotenv_values
from sqlalchemy.dialects import mysql
import logging

# use env
config_env = dotenv_values(".env")

# create engine 73
db73_host = config_env['DB73_HOST']
db73_username = config_env['DB73_USERNAME']
db73_password = config_env['DB73_PASSWORD']
db73_name = config_env['DB73_NAME']
db73_port = int(config_env['DB73_PORT'])

db73_password_encoded = urllib.parse.quote(db73_password)

# create engine 133
db133_host = config_env['DB133_HOST']
db133_username = config_env['DB133_USERNAME']
db133_password = config_env['DB133_PASSWORD']
db133_name = config_env['DB133_NAME']
db133_port = int(config_env['DB133_PORT'])

db133_password_encoded = urllib.parse.quote(db133_password)

# Construct the database URI with the encoded password
db73_uri = f"mysql+pymysql://{db73_username}:{db73_password_encoded}@{db73_host}:{db73_port}/{db73_name}"
engine73 = create_engine(db73_uri)

db133_uri = f"mysql+pymysql://{db133_username}:{db133_password_encoded}@{db133_host}:{db133_port}/{db133_name}"
engine133 = create_engine(db133_uri)

# connect to the database
try:
    # Test the connection
    engine73.connect()
    print('Database 73 Connected Successfully')
except Exception as e:
    print('Error Connecting to Database 73')
    print(e)


def main():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # Set desired logging level

    try:
        query = """
            SELECT imc_irf.HCODE, imc_irf.HN, imc_irf.AN,
                   imc_irf.DATE_REFER, ifnull(imc_irf.REFER,'0') as REFER,
                   imc_irf.REFERTYPE, imc_irf.PERSON_ID
            FROM imc_irf
            WHERE imc_irf.tupdate IS NULL
        """

        with engine73.connect() as con73:
            # Fetch data into a DataFrame
            df = pd.read_sql_query(query, con73)

        with engine133.connect() as con133:
            # insert ignore into irf
            # if_exists to insert ignore into the table
            df.to_sql('irf', con133, if_exists='replace', index=False)
            print("Data inserted successfully!")

    except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.ProgrammingError) as e:
        logger.error(f"Database error occurred: {e}")

    except sqlalchemy.exc.IntegrityError as e:
        logger.error(f"Integrity error occurred: {e}")
        logger.warning("Some rows failed to be inserted due to integrity constraints.")

    except Exception as e:

        logger.exception(f"Unexpected error: {e}")

    query2 = """
            SELECT imc_evl.HCODE,  imc_evl.HN,  imc_evl.AN,  imc_evl.DATE_SERV,  imc_evl.ITEM,
            imc_evl.RESULT,  imc_evl.D_UPDATE,  imc_evl.PERSON_ID FROM imc_evl where  imc_evl.tupdate is null
            """

    query3 = """
                SELECT
                   imc_ipd.HCODE, 
                   imc_ipd.HN, 
                   imc_ipd.AN, 
                   imc_ipd.DATEADM, 
                   imc_ipd.TIMEADM, 
                   imc_ipd.DATEDSC, 
                   imc_ipd.TIMEDSC, 
                   imc_ipd.DISCHS, 
                   imc_ipd.DISCHT, 
                   imc_ipd.WARDDSC, 
                   imc_ipd.DEPT, 
                   imc_ipd.ADM_W, 
                   imc_ipd.PERSON_ID
                   FROM imc_ipd
                   WHERE imc_ipd.tupdate IS NULL
            """


    query4 = """
                SELECT 
                  imc_pat.HCODE, 
                  imc_pat.HN, 
                  imc_pat.CHANGWAT, 
                  imc_pat.AMPHUR, 
                  imc_pat.DOB, 
                  imc_pat.SEX, 
                  imc_pat.MARRIAGE, 
                  imc_pat.OCCUPA, 
                  imc_pat.NATION, 
                  imc_pat.PERSON_ID, 
                  imc_pat.NAMEPAT, 
                  imc_pat.TITLE, 
                  imc_pat.FNAME, 
                  imc_pat.LNAME, 
                  imc_pat.IDTYPE, 
                  imc_pat.ADDRESS, 
                  imc_pat.AREA, 
                  imc_pat.TEL 
                  FROM 
                  imc_pat 
                  WHERE 
                  imc_pat.tupdate IS NULL
                """



if __name__ == '__main__':
    main()
