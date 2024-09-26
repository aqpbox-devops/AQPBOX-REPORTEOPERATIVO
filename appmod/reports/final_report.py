import pandas as pd
from datetime import datetime
from typing import Tuple, Literal
from appmod.data_getter.sql_handler import OpeProdDB
from appmod.data_getter.email_get_adj import extract_xlsx_attachments
from appmod.thisconstants.vars import *
from appmod.thisconstants.classes import *

TNAME = 'NAME'
TUSER = 'USER'
TCODE = 'CODE'

def get_all_report_by_analist(key: Literal['user', 'name', 'code'], 
                              analist_id: str, 
                              db_handler: OpeProdDB, 
                              snapshot_range: Tuple[datetime, datetime],
                              SECRETS: SecretsVault):

    srows = db_handler.query_row(key, analist_id, snapshot_range)

    if not is_end_of_month(snapshot_range[1]):
        extract_xlsx_attachments(SECRETS.recover(K_EMAIL), SECRETS.recover(K_PASSWORD), 
                                 DN_OPERATIVE)
        extract_xlsx_attachments(SECRETS.recover(K_EMAIL), SECRETS.recover(K_PASSWORD), 
                                 DN_PRODUCTIVITY)

    head = srows.iloc[0]

    data = FinalReportData(head[NAME], head[CODE], head[USER], head[CATEGORY], head[REGION],
                           head[ZONE], head[AGENCY], head[COMMITTEE], head[IN_DATE])
    
    for row in srows.iterrows():
        data.add_month(row)

    data.calculate_summary()

    return data

def get_report_from_analist(analist: str, 
                            input_t: str = Literal[TNAME, TUSER, TCODE], 
                            flatten_reope: pd.DataFrame = None, 
                            flatten_repro: pd.DataFrame = None):
    data = {}

    operative_cols = ['REGION', 'ZONE', 'AGENCY',
                      'COMMITTEE', TUSER, TCODE, TNAME,
                      'CATEGORY', 'IN_DATE',
                      'B1', 'A1', 'D3', 'B2', 'A2',
                      'A3', 'B3', 'D1', 'D2', 'E1', 'E2'
                     ]
    
    productivity_cols = ['REGION', 'ZONE', 'AGENCY',
                         'COMMITTEE', TUSER, 'C1']
    
    try:
        flatten_reope.columns = operative_cols
        flatten_repro.columns = productivity_cols

        srow = flatten_reope.loc[flatten_reope[input_t] == analist]
        srow['C1'] = flatten_repro.loc[flatten_repro[TUSER].isin(srow[TUSER]), 'C1'].values

        data['Crecimiento Anual Saldo'] = {
            'Saldo':srow['A1'],
            'Crecimiento':srow['A2'],
            'Meta':srow['A3'],
            'Diferencia':srow['A2'] - srow['A3'],
            '% Cumplimiento':srow['A2'] / srow['A3']
        }

        data['Crecimiento Anual Clientes'] = {
            'N°':srow['B1'],
            'Crecimiento':srow['B2'],
            'Meta':srow['B3'],
            'Diferencia':srow['B2'] - srow['B3'],
            '% Cumplimiento':srow['B2'] / srow['B3']
        }

        data['Productividad'] = {
            'N°':srow['C1'],
            '% Cumplimiento':srow['C1'] / 25.0
        }

        data['Indicadores'] = {
            '% Retencion':srow['D1'],
            '% Pago a Hoy':srow['D2'],
            'Mora SBS':srow['D3']
        }

        data['Puntaje'] = {
            'Total':srow['E1'],
            'Calificador':srow['E2']
        }
        
        return data
    
    except:
        print('Error')
    

