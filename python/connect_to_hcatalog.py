import os
import sys
import yaml
import mysql.connector

# connects to HCatalog and retrieves column metadata
def main(env):

    working_path = os.path.dirname(os.path.realpath(__file__))
    filename = os.path.join(working_path, 'config.yml')

    yaml_example = """
      hcatalog:
        dev:
          host: 127.0.0.1
          database: hive
          user: your_app
          pw: your_pw

        test:
          host: localhost
          database: hcat_meta
          user: your_app
          pw: your_pw
    """

    cfg = yaml.load(yaml_example)

    hcat_host = cfg['hcatalog'].get(env).get('host')
    hcat_db = cfg['hcatalog'].get(env).get('database')
    hcat_user = cfg['hcatalog'].get(env).get('user')
    hcat_pw = cfg['hcatalog'].get(env).get('pw')
    print("\nconnecting to [{host}].[{db}] as [{user}]".format(host=hcat_host, db=hcat_db, user=hcat_user))

    hcat_cxn = mysql.connector.connect(user=hcat_user, password=hcat_pw, host=hcat_host, database=hcat_db)
    hcat_cursor = hcat_cxn.cursor()

    hcat_query = "SELECT " \
                 "  d.NAME AS hive_db, " \
                 "  t.TBL_NAME AS hive_table " \
                 "FROM DBS AS d " \
                 "INNER JOIN TBLS AS t " \
                 "  ON d.DB_ID = t.DB_ID "

    hcat_cursor.execute(hcat_query)
    rows = hcat_cursor.fetchall()

    tables = [(bytes(row[0]), bytes(row[1])) for row in rows]
    print("\nThere are [{}] tables available in Hive:".format(len(tables)))

    for table in tables:
        print("  {hive_db}.{hive_table}".format(hive_db=table[0],hive_table=table[1]))

    hcat_cursor.close()
    hcat_cxn.close()

# canonical script entry point
if __name__ == '__main__':
    sys.exit(main(env='dev'))