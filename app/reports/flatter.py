import pandas as pd
from thisutils import *
import io

outpul_cols_reop = [
    "LABEL(-)",
    "REGION",
    "ZONA",
    "AGENCIA",
    "COMITE",
    "ANALISTA",
    "CODIGO",
    "NOMBRE",
    "CATEGORIA",
    "TIPO",
    "DIAS TRABAJADOS",
    "Fecha Ingreso",
    "Saldos al dia Clientes",#B1
    "Saldos al dia Monto",#A1
    "Saldos al dia Saldo Promedio",
    "Saldos al dia Mora SBS",#E1
    "Variacion Anual Cartera Bruta Clientes",
    "Variacion Anual Cartera Bruta Monto",
    "Variacion Mensual Cartera Bruta Clientes",#B2
    "Variacion Mensual Cartera Bruta Monto",#A2
    "Variacion Dia Cartera Bruta Clientes",
    "Variacion Dia Cartera Bruta Monto",
    "Nuevos No.",
    "Nuevos Monto",
    "Recurrentes No.",
    "Recurrentes Monto",
    "Total No.",
    "Total Monto",
    "Micro Monto",
    "Micro %",
    "TEA %",
    "Cartera al dia (0 dias) Saldo",
    "Cartera al dia (0 dias) %",
    "Mora de 1-8 Saldo",
    "Mora de 1-8 %",
    "Mora 9-30 Saldo",
    "Mora 9-30 %",
    "Mora > 30 Saldo",
    "Mora > 30 %",
    "Mora > 8 Saldo",
    "Mora > 8 %",
    "Mora > 8 + Castigos + Venta Saldo",
    "Mora > 8 + Castigos + Venta %",
    "VAR MORA > 8",
    "% VAR MORA > 8",
    "Variacion Mora > 30 + Castigos + Venta Saldo",
    "Variacion Mora > 30 + Castigos + Venta %",
    "Tramo de 1 a 30 Base a contener (inicio de mes)",
    "Tramo de 1 a 30 Cartera Contenida",
    "Tramo de 1 a 30 Cartera Deteriorada",
    "Tramo de 1 a 30 % Monto Contenido",
    "MONTO A CONTENER 31 A 60",
    "Tramo de 31 a 60 Cartera Contenida y Liberada",
    "Tramo de 31 a 60 Cartera Deteriorada",
    "%CONTENIDO 31 A 60",
    "SALDO META",#A3
    "LOGRO SALDO",
    "CLIENTES META",#B3
    "LOGRO CLIENTES",
    "Mora Contable Meta",
    "Mora Contable Logro (%)",
    "CLIENTES A RETENER",
    "CLIENTES RETENIDOS",
    "% RETENCION",#D1
    "Retencion de Clientes de Alto Valor No. Base",
    "Retencion de Clientes de Alto Valor No. Retenidos",
    "Retencion de Clientes de Alto Valor % Retencion",
    "No. Pagos Programados del Mes",
    "No. Pagos Programados al Dia",
    "No. Pagos Ejecutados a Hoy",
    "% Pago a Hoy",#D2
    "Crecimiento Saldo",
    "Crecimiento Clientes",
    "Retencion",
    "Contencion de 31 a 60",
    "Cartera > a 8",
    "Productividad",
    "Puntaje Total",#E2
    "Calificacion"#E3
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

def operative_from_excel(content_buffer: io.BytesIO):

    big_table = pd.read_excel(content_buffer, 
                              header=[4,5,6,7,8,9], index_col=0)

    big_table.columns = big_table.columns.droplevel(-1)

    column_ranges = [(0, 6), (6, 16), (16, 25), (25, 41), (41, 49), (49, 61),
                    (61, 65), (65, 73)]
    group_names = ['Datos Analista', 'Cartera Bruta', 'Desembolso Acumulado Mensual',
                  'Calidad de Cartera', 'Gestion por Tramos de Cartera',
                  'Retencion de Clientes', 'Seguimiento de Pagos', 'Puntaje']

    big_table_d = split_df_by_column_ranges(big_table, column_ranges, group_names)

    big_table_d['Datos Analista'] = big_table_d['Datos Analista'].droplevel(level=[0,1,2,3], axis=1)
    big_table_d['Cartera Bruta'] = big_table_d['Cartera Bruta'].droplevel(level=[0,1,3], axis=1)
    big_table_d['Desembolso Acumulado Mensual'] = big_table_d['Desembolso Acumulado Mensual'].droplevel(level=[0,1,2], axis=1)
    big_table_d['Calidad de Cartera'] = big_table_d['Calidad de Cartera'].droplevel(level=[0,1,3], axis=1)
    big_table_d['Gestion por Tramos de Cartera'] = big_table_d['Gestion por Tramos de Cartera'].droplevel(level=[0,1], axis=1)#
    big_table_d['Retencion de Clientes'] = big_table_d['Retencion de Clientes'].droplevel(level=[0,1,3], axis=1)
    big_table_d['Seguimiento de Pagos'] = big_table_d['Seguimiento de Pagos'].droplevel(level=[0,1,2], axis=1)#
    big_table_d['Puntaje'] = big_table_d['Puntaje'].droplevel(level=[0,1,2], axis=1)#

    for group in group_names:
      big_table_d[group] = merge_multicolumns(big_table_d[group])
      big_table_d[group] = create_hierarchical_rows(big_table_d[group])

    n = 6
    pre_colx = list(big_table_d.values())[0].iloc[:n]

    output = pd.concat([pre_colx] + list(big_table_d.values()), ignore_index=True)

    output = flatten_rows(output, n)

    output.columns = outpul_cols_reop

    output = output.dropna(subset=['SALDO META'])

    output = output[["REGION", "ZONA", "AGENCIA",
                     "COMITE", "ANALISTA", "CODIGO", "NOMBRE",
                     "CATEGORIA", "Fecha Ingreso",
                     "Saldos al dia Clientes",
                     "Saldos al dia Monto",
                     "Saldos al dia Mora SBS",
                     "Variacion Mensual Cartera Bruta Clientes",
                     "Variacion Mensual Cartera Bruta Monto",
                     "SALDO META", "CLIENTES META",
                     "% RETENCION", "% Pago a Hoy", "Puntaje Total",
                     "Calificacion"
                    ]]

    return output.reset_index(drop=True)

def productivity_from_excel(content_buffer: io.BytesIO):

  big_table = pd.read_excel(content_buffer, 
                            header=[5,6,7,8,9], index_col=0)

  big_table.columns = big_table.columns.droplevel(-1)

  column_ranges = [(0,1)]
  group_names = ['KPI de Productividad']

  big_table_d = split_df_by_column_ranges(big_table, column_ranges, group_names)
  big_table_d['KPI de Productividad'] = big_table_d['KPI de Productividad'].droplevel(level=[1,3], axis=1)

  for group in group_names:
    big_table_d[group] = merge_multicolumns(big_table_d[group])
    big_table_d[group] = create_hierarchical_rows(big_table_d[group])

  n = 6
  pre_colx = list(big_table_d.values())[0].iloc[:n]

  output = pd.concat([pre_colx] + list(big_table_d.values()), ignore_index=True)

  output = flatten_rows(output, n)

  output.columns = outpul_cols_repr

  output = output[['REGION', 'ZONA', 'AGENCIA',
                   'COMITE', 'ANALISTA',
                   'Total Operaciones Productivas']]

  return output.reset_index(drop=True)