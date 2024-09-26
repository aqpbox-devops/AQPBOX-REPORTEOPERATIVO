import pandas as pd
from datetime import datetime
from appmod.reports.thisutils import *
from appmod.thisconstants.vars import *
import io
import sys

outpul_cols_reop = [
    'LABEL(-)',
    'REGION',
    'ZONA',
    'AGENCIA',
    'COMITE',
    'ANALISTA',
    'CODIGO',
    'NOMBRE',
    'CATEGORIA',
    'TIPO',
    'DIAS TRABAJADOS',
    'Fecha Ingreso',
    'Saldos al dia Clientes',#B1
    'Saldos al dia Monto',#A1
    'Saldos al dia Saldo Promedio',
    'Saldos al dia Mora SBS',#E1
    'Variacion Anual Cartera Bruta Clientes',
    'Variacion Anual Cartera Bruta Monto',
    'Variacion Mensual Cartera Bruta Clientes',#B2
    'Variacion Mensual Cartera Bruta Monto',#A2
    'Variacion Dia Cartera Bruta Clientes',
    'Variacion Dia Cartera Bruta Monto',
    'Nuevos No.',
    'Nuevos Monto',
    'Recurrentes No.',
    'Recurrentes Monto',
    'Total No.',
    'Total Monto',
    'Micro Monto',
    'Micro %',
    'TEA %',
    'Cartera al dia (0 dias) Saldo',
    'Cartera al dia (0 dias) %',
    'Mora de 1-8 Saldo',
    'Mora de 1-8 %',
    'Mora 9-30 Saldo',
    'Mora 9-30 %',
    'Mora > 30 Saldo',
    'Mora > 30 %',
    'Mora > 8 Saldo',
    'Mora > 8 %',
    'Mora > 8 + Castigos + Venta Saldo',
    'Mora > 8 + Castigos + Venta %',
    'VAR MORA > 8',
    '% VAR MORA > 8',
    'Variacion Mora > 30 + Castigos + Venta Saldo',
    'Variacion Mora > 30 + Castigos + Venta %',
    'Tramo de 1 a 30 Base a contener (inicio de mes)',
    'Tramo de 1 a 30 Cartera Contenida',
    'Tramo de 1 a 30 Cartera Deteriorada',
    'Tramo de 1 a 30 % Monto Contenido',
    'MONTO A CONTENER 31 A 60',
    'Tramo de 31 a 60 Cartera Contenida y Liberada',
    'Tramo de 31 a 60 Cartera Deteriorada',
    '%CONTENIDO 31 A 60',
    'SALDO META',#A3
    'LOGRO SALDO',
    'CLIENTES META',#B3
    'LOGRO CLIENTES',
    'Mora Contable Meta',
    'Mora Contable Logro (%)',
    'CLIENTES A RETENER',
    'CLIENTES RETENIDOS',
    '% RETENCION',#D1
    'Retencion de Clientes de Alto Valor No. Base',
    'Retencion de Clientes de Alto Valor No. Retenidos',
    'Retencion de Clientes de Alto Valor % Retencion',
    'No. Pagos Programados del Mes',
    'No. Pagos Programados al Dia',
    'No. Pagos Ejecutados a Hoy',
    '% Pago a Hoy',#D2
    'Crecimiento Saldo',
    'Crecimiento Clientes',
    'Retencion',
    'Contencion de 31 a 60',
    'Cartera > a 8',
    'Productividad',
    'Puntaje Total',#E2
    'Calificacion'#E3
]

outpul_cols_repr = [
  'LABEL(-)',
  'REGION',
  'ZONA',
  'AGENCIA',
  'COMITE',
  'ANALISTA',
  'Total Operaciones Productivas'#C1
]

def flat_operative_from_excel(content_buffer: io.BytesIO):

    big_table = pd.read_excel(content_buffer, header=None, nrows=3)

    snapshot_date = datetime.strptime(big_table.iloc[1, 0].split(': ')[1], '%d/%m/%Y')

    big_table = pd.read_excel(content_buffer, header=[4,5,6,7,8,9], index_col=0, engine='openpyxl')

    big_table.columns = big_table.columns.droplevel(-1)

    big_table = select_hierarchy(big_table, 'TOTAL CAJA')

    big_table.columns = range(big_table.shape[1])

    output = create_hierarchical_rows(big_table)

    output.columns = outpul_cols_reop

    output = output.dropna(subset=['SALDO META'])

    output[SNAPSHOT_DATE] = snapshot_date

    output = output[[SNAPSHOT_DATE, 'REGION', 'ZONA', 'AGENCIA',
                     'COMITE', 'ANALISTA', 'CODIGO', 'NOMBRE',
                     'CATEGORIA', 'Fecha Ingreso',
                     'Saldos al dia Clientes',
                     'Saldos al dia Monto',
                     'Saldos al dia Mora SBS',
                     'Variacion Mensual Cartera Bruta Clientes',
                     'Variacion Mensual Cartera Bruta Monto',
                     'SALDO META', 'CLIENTES META',
                     '% RETENCION', '% Pago a Hoy', 'Puntaje Total',
                     'Calificacion'
                    ]]

    return output.reset_index(drop=True)

def flat_productivity_from_excel(content_buffer: io.BytesIO):
  
    big_table = pd.read_excel(content_buffer, header=None, nrows=3)

    snapshot_date = datetime.strptime(big_table.iloc[1, 0].split(': ')[1], '%d/%m/%Y')

    big_table = pd.read_excel(content_buffer, header=[5,6,7,8,9], index_col=0, engine='openpyxl')
    
    big_table.columns = big_table.columns.droplevel(-1)

    big_table.columns = range(big_table.shape[1])

    output = big_table.iloc[:, :1]

    output = create_hierarchical_rows(output)

    output.columns = outpul_cols_repr

    output[SNAPSHOT_DATE] = snapshot_date

    output = output[[SNAPSHOT_DATE, 'REGION', 'ZONA', 'AGENCIA',
                    'COMITE', 'ANALISTA',
                    'Total Operaciones Productivas']]

    return output.reset_index(drop=True)

def get_data_opeprod(ope_buffer: io.BytesIO, prod_buffer: io.BytesIO):
    operative_snapshot = flat_operative_from_excel(ope_buffer)
    productivity_snapshot = flat_productivity_from_excel(prod_buffer)

    columns_to_use = [col for col in productivity_snapshot.columns if col not in operative_snapshot.columns or col == 'ANALISTA']

    result = pd.merge(operative_snapshot, productivity_snapshot[columns_to_use], on='ANALISTA', how='outer')

    result.columns = [
        SNAPSHOT_DATE,
        REGION,
        ZONE,
        AGENCY,
        COMMITTEE,
        USER,
        CODE,
        NAME,
        CATEGORY,
        IN_DATE,
        SADC,
        SADM,
        SBS,
        VMCBC,
        VMCBM,
        SMETA,
        CMETA,
        RET,
        PAH,
        SCORE,
        QUALI,
        TOPP
    ]

    return result.dropna(subset=[SNAPSHOT_DATE])

if __name__ == '__main__':
    import time
    print(sys.getdefaultencoding())

    opepath = r"C:\Users\IMAMANIH\Documents\local-python\big-excel\shared\REP_R327_OPERATIVO_GENERAL_RRHH_20240828.xlsx"
    prodpath = r"C:\Users\IMAMANIH\Documents\local-python\big-excel\shared\REP_R017_PRODUCTIVIDAD_20240828.xlsx"
    
    with open(opepath, 'rb') as f:
        ope_buffer = io.BytesIO(f.read())

    with open(prodpath, 'rb') as f:
        prod_buffer = io.BytesIO(f.read())

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)

    start_time = time.time()
    result = get_data_opeprod(ope_buffer, prod_buffer)
    print(f"Getter time: {time.time() - start_time:.4f} segundos")

    print(result.info())
    print(result.head(30))
