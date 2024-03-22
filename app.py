import pymysql
import urllib.parse
from datetime import datetime

from dotenv import dotenv_values

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

# connect database 73 with mysql
connection73 = pymysql.connect(
    host=db73_host,
    user=db73_username,
    password=db73_password,
    db=db73_name,
    port=db73_port
)

# connect database 133 with mysql
connection133 = pymysql.connect(
    host=db133_host,
    user=db133_username,
    password=db133_password,
    db=db133_name,
    port=db133_port
)

# check connection
if connection73.open:
    print("Connected to Database 73")
else:
    print("Error Connecting to Database 73")

if connection133.open:
    print("Connected to Database 133")
else:
    print("Error Connecting to Database 133")


def main():
    try:
        query = """SELECT imc_irf.HCODE, imc_irf.HN, imc_irf.AN, 
                     imc_irf.DATE_REFER, imc_irf.REFER, 
                     imc_irf.REFERTYPE, imc_irf.PERSON_ID 
                     FROM imc_irf WHERE imc_irf.tupdate IS NULL"""

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

        with connection73.cursor() as cursor:

            # insert to irf ################################################################################################
            cursor.execute(query)
            results = cursor.fetchall()

            rows = 0
            for row in results:
                with connection133.cursor() as cursor133:
                    date_refer_str = row[3].strftime("%Y-%m-%d %H:%M:%S")

                    sql = f"INSERT IGNORE INTO irf (HCODE, HN, AN, DATE_REFER, REFER, REFERTYPE, PERSON_ID) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    val = (row[0], row[1], row[2], date_refer_str, row[4], row[5], row[6])
                    print("sql : ", sql % val)
                    cursor133.execute(sql, val)
                    connection133.commit()
                    rows += 1

            print(f"Insert to irf with {rows} record inserted.")

            # insert to imc_evl ################################################################################################
            cursor.execute(query2)
            results = cursor.fetchall()
            rows = 0
            for row in results:
                with connection133.cursor() as cursor133:
                    date_serv_str = row[3].strftime("%Y-%m-%d %H:%M:%S")
                    d_update_str = row[6].strftime("%Y-%m-%d %H:%M:%S")

                    sql = f"INSERT IGNORE INTO evl (HCODE, HN, AN, DATE_SERV, ITEM, RESULT, D_UPDATE, PERSON_ID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    val = (row[0], row[1], row[2], date_serv_str, row[4], row[5], d_update_str, row[7])
                    print("sql : ", sql % val)
                    cursor133.execute(sql, val)
                    connection133.commit()
                    rows += 1

            print(f"Insert to imc_evl with {rows} record inserted.")

            # insert to ipd ################################################################################################
            cursor.execute(query3)
            results = cursor.fetchall()
            rows = 0
            for row in results:
                with connection133.cursor() as cursor133:
                    dateadm_str = row[3].strftime("%Y-%m-%d %H:%M:%S")
                    datedsc_str = row[5].strftime("%Y-%m-%d %H:%M:%S")

                    sql = f"INSERT IGNORE INTO ipd (HCODE, HN, AN, DATEADM, TIMEADM, DATEDSC, TIMEDSC, DISCHS, DISCHT, WARDDSC, DEPT, ADM_W, PERSON_ID) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    val = (row[0], row[1], row[2], dateadm_str, row[4], datedsc_str, row[6], row[7], row[8], row[9], row[10], row[11], row[12])
                    print("sql : ", sql % val)
                    cursor133.execute(sql, val)
                    connection133.commit()
                    rows += 1
            print(f"Insert to ipd with {rows} record inserted.")

            # insert to pat ################################################################################################
            cursor.execute(query4)
            results = cursor.fetchall()
            rows = 0
            for row in results:
                with connection133.cursor() as cursor133:
                    dob_str = row[4].strftime("%Y-%m-%d %H:%M:%S")

                    sql = """
                    INSERT IGNORE INTO pat (HCODE, HN, CHANGWAT, AMPHUR, DOB, SEX, MARRIAGE, OCCUPA, NATION, PERSON_ID, NAMEPAT, TITLE, FNAME, LNAME, IDTYPE, ADDRESS, AREA)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """
                    val = (row[0], row[1], row[2], row[3], dob_str, row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16])
                    print("sql : ", sql % val)
                    cursor133.execute(sql, val)
                    connection133.commit()
                    rows += 1
            print(f"Insert to pat with {rows} record inserted.")

    except Exception as e:
        print(e)

    finally:
        connection73.close()
        connection133.close()

if __name__ == "__main__":
    main()
