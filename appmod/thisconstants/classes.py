import pandas as pd
import locale
from typing import List
from datetime import datetime, timedelta
from appmod.thisconstants.vars import *

def set_locale():
    current_locale = locale.getlocale(locale.LC_TIME)
    desired_locale = 'es_ES.UTF-8'

    if current_locale != (desired_locale, 'UTF-8'):
        locale.setlocale(locale.LC_TIME, desired_locale)
        print(f"Locale set to {desired_locale}")
    else:
        print("Locale already set.")

def div_by_0(n, d):
    return 0 if d == 0 else (n / d)

def get_quali_label(number):
    for key in sorted(QUALI_LUT.keys(), key=int):
        if number <= int(key):
            return QUALI_LUT[key]
    return None

def is_end_of_month(date: datetime) -> bool:
    next_day = date + timedelta(days=1)
    return next_day.month != date.month

class SecretsVault:
    def __init__(self):
        self.secrets = {}

    def store(self, keyname, secret):
        self.secrets[keyname] = secret

    def recover(self, keyname):
        return self.secrets[keyname]

class MonthDataWriter:
    def __init__(self, month: str, 
                 dfA: pd.DataFrame, 
                 dfB: pd.DataFrame,
                 dfC: pd.DataFrame,
                 dfD: pd.DataFrame,
                 dfE: pd.DataFrame):
        self.id = month.upper()
        self.A = dfA
        self.B = dfB
        self.C = dfC
        self.D = dfD
        self.E = dfE
        
    def add2A(self, balance, growth, goal):
        row = [balance, growth, goal]
        difference = growth - goal
        compliance = div_by_0(growth, goal)

        row += [difference, compliance]

        self.A.loc[self.id] = row

    def add2B(self, clients, growth, goal):
        row = [clients, growth, goal]
        difference = growth - goal
        compliance = div_by_0(growth, goal)

        row += [difference, compliance]

        self.B.loc[self.id] = row

    def add2C(self, kpi):
        self.C.loc[self.id] = [kpi, kpi / 25.0]

    def add2D(self, retention, payment_to_date, sbs):
        self.D.loc[self.id] = [retention, payment_to_date, sbs]

    def add2E(self, score, qualifier):
        self.E.loc[self.id] = [score, qualifier]

class FinalReportData:
    def __init__(self, name, 
                 code, 
                 user, 
                 category, 
                 region,
                 zone,
                 agency,
                 committee, 
                 in_date):
        self.name = name
        self.code = code
        self.user = user
        self.category = category
        self.region = region
        self.zone = zone
        self.agency = agency
        self.committee = committee
        self.time_working = datetime.now() - in_date

        set_locale()

        all_months = [str(datetime(2021, i, 1).strftime('%B')).upper() for i in range(1, 13)]
        all_months += [LSUMMARY]

        self.A = pd.DataFrame(index=all_months, columns=[LBALANCE, LGROWTH, LGOAL, LDIFF, LCOMPL])
        self.B = pd.DataFrame(index=all_months, columns=[LNUMBERING, LGROWTH, LGOAL, LDIFF, LCOMPL])
        self.C = pd.DataFrame(index=all_months, columns=[LNUMBERING, LCOMPL])
        self.D = pd.DataFrame(index=all_months, columns=[LRETENTION, LPAY2DATE, LOVERDUE])
        self.E = pd.DataFrame(index=all_months, columns=[LTOTAL, LQUALIFIER])

    def add_month(self, row: pd.DataFrame):
        try:
            set_locale()
            month = MonthDataWriter(row[SNAPSHOT_DATE].strftime("%B"),
                            self.A, self.B, self.C, self.D, self.E)
            
            month.add2A(row[SADM], row[VMCBM], row[SMETA])
            month.add2B(row[SADC], row[VMCBC], row[CMETA])
            month.add2C(row[TOPP])
            month.add2D(row[RET], row[PAH], row[SBS])
            month.add2E(row[SCORE], row[QUALI])

        except KeyError:
            print('Key not found.')

    def calculate_summary(self):
        summA = {}
        summB = {}
        summC = {}
        summD = {}
        summE = {}

        summA[LBALANCE] = self.A.loc[self.A[LBALANCE].last_valid_index(), LBALANCE]
        summA[LGROWTH] = self.A[LGROWTH].sum()
        summA[LGOAL] = self.A[LGOAL].sum()
        summA[LDIFF] = self.A[LDIFF].sum()
        summA[LCOMPL] = div_by_0(summA[LGOAL], summA[LDIFF])

        summB[LNUMBERING] = self.B.loc[self.B[LNUMBERING].last_valid_index(), LNUMBERING]
        summB[LGROWTH] = self.B[LGROWTH].sum()
        summB[LGOAL] = self.B[LGOAL].sum()
        summB[LDIFF] = self.B[LDIFF].sum()
        summB[LCOMPL] = div_by_0(summB[LGOAL], summB[LDIFF])

        summC[LNUMBERING] = div_by_0(self.C[LNUMBERING].sum(), 
                                        ((self.C[LNUMBERING].notna()) & (self.C[LNUMBERING] != 0)).sum())
        summC[LCOMPL] = div_by_0(summC[LNUMBERING], 25.0)

        summD[LRETENTION] = div_by_0(self.D[LRETENTION].sum(), 
                                        ((self.D[LRETENTION].notna()) & (self.D[LRETENTION] != 0)).sum())
        summD[LPAY2DATE] = div_by_0(self.D[LPAY2DATE].sum(), 
                                        ((self.D[LPAY2DATE].notna()) & (self.D[LPAY2DATE] != 0)).sum())
        summD[LOVERDUE] = self.D.loc[self.D[LOVERDUE].last_valid_index(), LOVERDUE]
        summE[LTOTAL] = self.E[LTOTAL].mean(skipna=True)
        summE[LQUALIFIER] = get_quali_label(summE[LTOTAL])

        self.A.loc[LSUMMARY] = summA
        self.B.loc[LSUMMARY] = summB
        self.C.loc[LSUMMARY] = summC
        self.D.loc[LSUMMARY] = summD
        self.E.loc[LSUMMARY] = summE

if __name__ == '__main__':
    dt = datetime.now()  # Current date and time

    # Convert to string with full month name
    month_name = dt.strftime("%B") 
    locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')  # Change this based on your OS if needed

    # Convert to string with full month name in Spanish
    month_name_spanish = dt.strftime("%B")

    print(f"Full month name in English: {month_name}")
    print(f"Full month name in Spanish: {month_name_spanish}")
