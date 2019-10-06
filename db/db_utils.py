import sys
import logging
import db.rds_config as conf
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def practice_pal_connection():
    try:
        return pymysql.connect(conf.db_endpoint,
                               user=conf.db_username,
                               passwd=conf.db_password,
                               db=conf.db_name,
                               connect_timeout=15)
    except pymysql.MySQLError as e:
        logger.error("Unexpected error: Could not connect to MySQL instance")
        logger.error(e)
        sys.exit()


def create_table(statement):
    """
    create table in practice_pal database
    :param statement: str create table statement
    :return: str information regarding created table
    """
    conn = practice_pal_connection()
    with conn.cursor() as cur:
        cur.execute(statement)
    conn.commit()
    conn.close()

    split_word = 'table'
    bef, split_word, aft = statement.lower().partition(split_word)
    return 'Table "{}" successfully created'.format(aft.split()[0])


if __name__ == '__main__':
    table_statement = 'CREATE TABLE practice_drills ( drill_id int NOT NULL, name varchar(50) NOT NULL, display_name varchar(50) NOT NULL, skill_level varchar(40) NOT NULL, description varchar(255) NOT NULL, PRIMARY KEY (drill_id))'
    create_table(table_statement)
