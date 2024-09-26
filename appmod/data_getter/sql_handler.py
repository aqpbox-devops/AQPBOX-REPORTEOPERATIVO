import sqlite3
import pandas as pd
from typing import Tuple, Literal
from datetime import datetime
from appmod.thisconstants.vars import *

class OpeProdDB:
    def __init__(self, db_name: str = 'opeprod.db'):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self):
        """Create the T_OPEPROD table with specified columns."""
        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                {SNAPSHOT_DATE} DATETIME,
                {REGION} VARCHAR(15),
                {ZONE} VARCHAR(22),
                {AGENCY} VARCHAR(25),
                {COMMITTEE} VARCHAR(16),
                {USER} VARCHAR(30),
                {CODE} INTEGER,
                {NAME} VARCHAR(160),
                {CATEGORY} VARCHAR(35),
                {IN_DATE} DATETIME,
                {SADC} INTEGER,
                {SADM} INTEGER,
                {SBS} FLOAT,
                {VMCBC} INTEGER,
                {VMCBM} INTEGER,
                {SMETA} INTEGER,
                {CMETA} INTEGER,
                {RET} FLOAT,
                {PAH} FLOAT,
                {SCORE} FLOAT,
                {QUALI} VARCHAR(10),
                {TOPP} INTEGER
            )
        ''')
        self.conn.commit()

    def query_row(self, key: Literal['user', 'name', 'code'], 
                  analist_id: str, 
                  snapshot_range: Tuple[datetime, datetime]) -> pd.DataFrame:
        """Query a row based on a key and return the results in a DataFrame, ordered by snapshot_date."""
        query = f'''
            SELECT * FROM {TABLE_NAME}
            WHERE {key} = ? AND {SNAPSHOT_DATE} BETWEEN ? AND ?
            ORDER BY {SNAPSHOT_DATE}
        '''
        self.cursor.execute(query, (analist_id, 
                                    snapshot_range[0].strftime('%Y-%m-%d %H:%M:%S'), 
                                    snapshot_range[1].strftime('%Y-%m-%d %H:%M:%S')))
        rows = self.cursor.fetchall()

        columns = [desc[0] for desc in self.cursor.description]  # Get column names
        result = pd.DataFrame(rows, columns=columns)
        
        result[SNAPSHOT_DATE] = pd.to_datetime(result[SNAPSHOT_DATE])
        result[IN_DATE] = pd.to_datetime(result[IN_DATE])

        return result

    def insert_row(self, df_row: pd.Series):
        """Insert a row into the T_OPEPROD table only if snapshot_date is the last day of the month."""
        snapshot_date = df_row[SNAPSHOT_DATE]
        last_day_of_month = pd.Timestamp(snapshot_date).is_month_end

        if last_day_of_month:
            insert_query = f'''
                INSERT INTO {TABLE_NAME} ({SNAPSHOT_DATE}, {REGION}, {ZONE}, {AGENCY}, {COMMITTEE}, {USER}, {CODE}, {NAME}, {CATEGORY}, 
                                        {IN_DATE}, {SADC}, {SADM}, {SBS}, {VMCBC}, {VMCBM}, {SMETA}, {CMETA}, {RET}, {PAH}, {SCORE}, 
                                        {QUALI}, {TOPP})
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            self.cursor.execute(insert_query, tuple(df_row))
            self.conn.commit()
        else:
            raise ValueError("The snapshot_date is not the last day of the month.")

    def delete_rows_by_snapshot(self, snapshot_date: datetime):
        """Delete rows based on a specific snapshot_date."""

        delete_query = f'''
            DELETE FROM {TABLE_NAME}
            WHERE {SNAPSHOT_DATE} = ?
        '''
        self.cursor.execute(delete_query, (snapshot_date.strftime('%Y-%m-%d %H:%M:%S'),))
        self.conn.commit()

    def close(self):
        """Close the database connection."""
        self.cursor.close()
        self.conn.close()

    def destroy(self):
        drop_query = f"DROP TABLE IF EXISTS {TABLE_NAME};"
        self.cursor.execute(drop_query)
        self.conn.commit()
        self.close()


# Example usage
if __name__ == '__main__':
    db = OpeProdDB(r'C:\Users\IMAMANIH\Documents\GithubRepos\AQPBOX-REPORTEOPERATIVO\temp\dbs\test_db.db')


    # Example of inserting data
    row_data = pd.Series({
        'snapshot_date': '2023-08-31',
        'region': 'North',
        'zone': 'Zone A',
        'agency': 'Agency X',
        'committee': 'Comm1',
        'user': 'someone',
        'code': 101,
        'name': 'Product X',
        'category': 'Category A',
        'in_date': '2023-08-01',
        'sadc': 10,
        'sadm': 20,
        'sbs': 30.5,
        'vmcbc': 40,
        'vmcbm': 50,
        'smeta': 60,
        'cmeta': 70,
        'ret': 0.8,
        'pah': 0.9,
        'score': 85.5,
        'quali': 'High',
        'topp': 100
    })

    db.insert_row(row_data)

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)

    df = db.query_row(key=CODE, analist_id='101', snapshot_range=(datetime(2023, 1, 1), datetime(2023, 12, 31)))
    print(df)
    df = db.query_row(key=USER, analist_id='someone', snapshot_range=(datetime(2023, 1, 1), datetime(2023, 12, 31)))
    print(df)
    df = db.query_row(key=NAME, analist_id='Product X', snapshot_range=(datetime(2023, 1, 1), datetime(2023, 12, 31)))
    print(df)
    print(df.info())
    # Example of deleting rows
    db.delete_rows_by_snapshot(datetime(2023, 8, 31))

    db.destroy()
