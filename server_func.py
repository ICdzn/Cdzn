from . import db
import datetime
import xlrd, xlwt
import pandas as pd
import random

'''
company_id - int() - номер компании в таблице db.company
d1, d2 - str() - даты определяющие нужный промежуток времени - '2020-07-04'
basa - int() - значение, определяющие базу/таблицу с которой беруться данные:
    1 - 3 розділ
    2 - 4 розділ
    3 - Журнал виплат
    4 - Резерв заявлених збитків

Для 3 и 4 разделов d1 и d2 определяют кварталы, по которым будет создаваться DataFrame
Для журнала и резерва d1 и d2 сравниваются с датами страховых актов из таблиц db.payout и db.rezerv
'''
ir4_desc = pd.DataFrame(columns = db.ir4_description.fields[2:])
for i in db(db.ir4_description).select():
    ir4_desc.loc[ir4_desc.shape[0]] = [i.p_id, i.ekp, i.h015, i.k030, i.z220]
vid_s = pd.DataFrame(columns = ['nfp_id', 'nbu_id', 'iar_kod'])
for i in db((db.vid_strah.nfp_id != '_') | (db.vid_strah.iar_kod != '_')).select():
    vid_s.loc[vid_s.shape[0]] = [i.nfp_id, i.nbu_id, i.iar_kod]

def show_db(company_id, d1, d2, basa):
    if basa < 3:
        table = db.type
        q1, y1 = pd.to_datetime(d1, dayfirst=True).quarter, pd.to_datetime(d1).year
        q2, y2 = pd.to_datetime(d2, dayfirst=True).quarter, pd.to_datetime(d2).year
        quarters = (y2-y1)*4+q2-q1+1
        if basa == 1:
            main_df = new_nfp3()[4:6]
        else:
            main_df = new_nfp4()[4:6]
        main_df['quarter'] = ['квартал'] + [None for i in range(main_df.shape[0]-1)]
        main_df['year'] = ['рік'] + [None for i in range(main_df.shape[0]-1)] 
        for z in range(quarters):
            df = pd.DataFrame(columns=['ekp', 'h011', 'h015', 'k030', 'z220', 't100'])
            for i in db((db.type.quarter == (q1+z)%4) & (db.type.year == y1 + int((q1+z-1)/4)) & (db.type.company_id == company_id)).select():
                m = {'ekp': i.ekp, 'h011': i.h011, 'h015': i.h015, 'k030': i.k030, 'z220': i.z220, 't100': int(i.t100*100)}
                df.loc[df.shape[0]] = m
            df = db_toexcel(df, (q1+z)%4, y1 + int((q1+z-1)/4), basa)[6:]
            df['quarter'] = [(q1+z)%4 for i in range(df.shape[0])]
            df['year'] = [y1 + int((q1+z-1)/4) for i in range(df.shape[0])]
            main_df = main_df.append(df)
        df = main_df
    elif basa == 3:
        table = db.payout
        cols = table.fields[1:-1] + ['company']
        df = pd.DataFrame(columns=cols)
    else:
        table = db.rezerv
        cols = table.fields[1:-1] + ['company']
        df = pd.DataFrame(columns=cols)
    if basa > 2:
        if type(d1) != type(datetime.date.today()):
            d1 = pd.to_datetime(d1, dayfirst=True)
        if type(d2) != type(datetime.date.today()):
            d2 = pd.to_datetime(d2, dayfirst=True)
        for i in db(table.company_id == company_id).select():
            if len(i.insurance_act_date) != 10:
                continue
            date = pd.to_datetime(i.insurance_act_date, dayfirst=True)
            if date >= d1 and date <= d2:
                m = {'insurance_type': i.insurance_type, 'contract_num': i.contract_num, 'case_num': i.case_num,
                     'insurance_case_date': pd.to_datetime(i.insurance_case_date, dayfirst=True), 'statement_date': pd.to_datetime(i.statement_date, dayfirst=True),
                     'requirement_date': pd.to_datetime(i.requirement_date, dayfirst=True), 'insurance_act_date': date,
                     'insurance_payment_date': pd.to_datetime(i.insurance_payment_date, dayfirst=True), 'company': company_id,
                     'insurance_payment_size': i.insurance_payment_size}
                if basa == 3:
                    m['settlement_costs'] = i.settlement_costs
                else:
                    m['reserve_size'] = i.reserve_size
                    m['insert_date'] = pd.to_datetime(i.insert_date, dayfirst=True)
                df.loc[df.shape[0]] = m
    return df

def new_nfp3():
    ex3 = pd.ExcelFile("apps/neww/static/Раздел 3.xlsx").parse(0)
    ex3.columns = ['name', 'p_id', 'total'] + [str(i+101) for i in range(22)]
    a = {}
    for i in range(ex3.shape[0]):
        try:
            c = int(ex3.iloc[i]['p_id'])
        except ValueError:
            c = -10+i
        a.update({ex3.iloc[i].name: c})
    ex3 = ex3.rename(index = a)
    return ex3

def new_nfp4():
    ex4 = pd.ExcelFile("apps/neww/static/Раздел 4.xlsx").parse(0)
    ex4.columns = ['name', 'p_id', 'total'] + [str(i+201) for i in range(41)]
    a = {}
    for i in range(ex4.shape[0]):
        try:
            c = int(ex4.iloc[i]['p_id'])
        except ValueError:
            c = -10+i
        a.update({ex4.iloc[i].name: c})
    ex4 = ex4.rename(index = a)
    return ex4

def new_iar3():
    ex3_iar = pd.ExcelFile("apps/neww/static/iar_blank.xls").parse(1)
    ex3_iar.columns = ['тех стр', 'name', 'ист-к инф.', 'p_id', 'year', 'strahovshik'
    , 'total_pass', 'total'] + ["д0{}".format(i+1) for i in range(9)] + ["д{}".format(i+10) for i in range(11)
    ] + ['in_dobr', 'kod poln', 'н01', 'н02']
    a = {}
    for i in range(ex3_iar.shape[0]):
        try:
            c = int(ex3_iar.iloc[i]['p_id'])
        except ValueError:
            c = -10+i
        a.update({ex3_iar.iloc[i].name: c})
    ex3_iar = ex3_iar.rename(index = a)
    return ex3_iar

def new_iar4():
    ex4_iar = pd.ExcelFile("apps/neww/static/iar_blank.xls").parse(2)
    ex4_iar.columns = ['тех стр', 'name', 'ист-к инф.', 'p_id', 'year', 'strahovshik'
    , 'total_pass', 'total'] + ["о0{}".format(i+1) for i in range(9)] + ["о{}".format(i+10) for i in range(32)
    ] + ['ob_goz', 'kod poln'] + ["н0{}".format(i+3) for i in range(7)]
    a = {}
    for i in range(ex4_iar.shape[0]):
        try:
            c = int(ex4_iar.iloc[i]['p_id'])
        except ValueError:
            c = -10+i
        a.update({ex4_iar.iloc[i].name: c})
    ex4_iar = ex4_iar.rename(index = a)
    return ex4_iar

def db_toexcel(df, quarter, year, basa):
    if basa == 3:
        log15 = open('log15.txt', 'w')
    if basa > 2:
        if quarter == 4:
            period = str(year)
        else:
            period = "{0}-{1}м".format(year, quarter*3)
    if basa == 1:
        ex = new_nfp3()
        d = 3
    elif basa == 2:
        ex = new_nfp4()
        d = 3
    elif basa == 3:
        ex = new_iar3()
        d = 8
    else:
        ex = new_iar4()
        d = 8
    for i in range(df.shape[0]):
        p_id = ir4_desc[ir4_desc['ekp'] == df.iloc[i]['ekp']][ir4_desc['h015'] == df.iloc[i]['h015']
        ][ir4_desc['k030'] == df.iloc[i]['k030']][ir4_desc['z220'] == df.iloc[i]['z220']]['p_id']
        if len(p_id) == 0:
            pass
        else:
            p_id = int(p_id.iloc[0])
        if basa < 3:
            try:
                nfp = vid_s[vid_s['nbu_id'] == str(df.iloc[i]['h011'])]['nfp_id'].iloc[0]
            except IndexError:
                pass
            else:
                if nfp[0] == str(basa):
                    ex.loc[p_id][nfp] = df.iloc[i]['t100']
        else:
            try:
                iar = vid_s[vid_s['nbu_id'] == str(int(df.iloc[i]['h011']))]['iar_kod'].iloc[0]
            except IndexError:
                pass
            else:
                if basa == 3 and df.iloc[i]['h011'] == '06':
                    log15.write(str(iar)+'\n')
                if basa == 3 and (iar[0] == 'д' or iar in ['н01', 'н02']):
                    ex.loc[p_id][iar] = df.iloc[i]['t100']
                elif basa == 4 and (iar[0] == 'о' or iar in ['н03', 'н04', 'н05', 'н06', 'н07', 'н08', 'н09']):
                    ex.loc[p_id][iar] = df.iloc[i]['t100']
    if basa == 3:
        for i1 in range(6):
            log15.write(str(df.sort_values(by=['h011']).iloc[i1].to_dict())+'\n')
        log15.write(str(ex.loc[13][['д01', 'д02', 'д03', 'д04']]))
        log15.write(str(ex.loc[50][['д01', 'д02', 'д03', 'д04']]))
        log15.write(str(ex.loc[180][['д01', 'д02', 'д03', 'д04']]))
        log15.write(str(ex.loc[200][['д01', 'д02', 'д03', 'д04']]))
    for i1 in ir4_desc['p_id']:
        ex['total'][i1] = 0
        for i2 in ex.columns[d:]:
            try:
                if i1 in [90, 170, 171, 172, 190, 191]:
                    ex[i2][i1] = int(ex[i2][i1])
                else:
                    ex[i2][i1] = float(int(ex[i2][i1]/1000))/100
                ex['total'][i1] += ex[i2][i1]
            except ValueError:
                ex[i2][i1] = 0
    for i1 in ir4_desc['p_id']:
        if ex['total'][i1] == 0:
            if i1 == 10:
                if ex['total'][11] == 0:
                    for i2 in ex.columns[d-1:]:
                        ex[i2][11] = ex[i2][12] + ex[i2][13] + ex[i2][14]
                if ex['total'][15] == 0:
                    for i2 in ex.columns[d-1:]:
                        ex[i2][15] = ex[i2][16] + ex[i2][17] + ex[i2][18]
                for i2 in ex.columns[d-1:]:
                    ex[i2][10] = ex[i2][11] + ex[i2][15]
            elif i1 == 11:
                for i2 in ex.columns[d-1:]:
                    ex[i2][11] = ex[i2][12] + ex[i2][13] + ex[i2][14]
            elif i1 == 15:
                for i2 in ex.columns[d-1:]:
                    ex[i2][15] = ex[i2][16] + ex[i2][17] + ex[i2][18]
            elif i1 == 20:
                if ex['total'][21] == 0:
                    for i2 in ex.columns[d-1:]:
                        ex[i2][21] = ex[i2][22] + ex[i2][23]
                if ex['total'][25] == 0:
                    for i2 in ex.columns[d-1:]:
                        ex[i2][25] = ex[i2][26] + ex[i2][27]
                for i2 in ex.columns[d-1:]:
                    ex[i2][20] = ex[i2][21] + ex[i2][24] + ex[i2][25] + ex[i2][28]
            elif i1 == 21:
                for i2 in ex.columns[d-1:]:
                    ex[i2][21] = ex[i2][22] + ex[i2][23]
            elif i1 == 25:
                for i2 in ex.columns[d-1:]:
                    ex[i2][25] = ex[i2][26] + ex[i2][27]
            if i1 == 30:
                for i2 in ex.columns[d-1:]:
                    ex[i2][30] = ex[i2][31]
            if i1 == 40:
                for i2 in ex.columns[d-1:]:
                    ex[i2][40] = ex[i2][41]
            if i1 == 60:
                for i2 in ex.columns[d-1:]:
                    ex[i2][60] = ex[i2][61]
            if i1 == 70:
                for i2 in ex.columns[d-1:]:
                    ex[i2][70] = ex[i2][71] + ex[i2][72] + ex[i2][73] + ex[i2][74]
            if i1 == 80:
                for i2 in ex.columns[d-1:]:
                    ex[i2][80] = ex[i2][81]
            elif i1 == 100:
                if ex['total'][101] == 0:
                    for i2 in ex.columns[d-1:]:
                        ex[i2][101] = ex[i2][102] + ex[i2][103] + ex[i2][104]
                if ex['total'][105] == 0:
                    for i2 in ex.columns[d-1:]:
                        ex[i2][105] = ex[i2][106] + ex[i2][107] + ex[i2][108]
                for i2 in ex.columns[d-1:]:
                    ex[i2][100] = ex[i2][101] + ex[i2][105]
            elif i1 == 101:
                for i2 in ex.columns[d-1:]:
                    ex[i2][101] = ex[i2][102] + ex[i2][103] + ex[i2][104]
            elif i1 == 105:
                   for i2 in ex.columns[d-1:]:
                       ex[i2][105] = ex[i2][106] + ex[i2][107] + ex[i2][108]
            if i1 == 110:
                for i2 in ex.columns[d-1:]:
                    ex[i2][110] = ex[i2][111]
            if i1 == 130:
                for i2 in ex.columns[d-1:]:
                    ex[i2][130] = ex[i2][131] + ex[i2][132]
            if i1 == 140:
                for i2 in ex.columns[d-1:]:
                    ex[i2][140] = ex[i2][141] + ex[i2][142] + ex[i2][143] + ex[i2][144]
            if i1 == 150:
                for i2 in ex.columns[d-1:]:
                    ex[i2][150] = ex[i2][151] + ex[i2][152] + ex[i2][153] + ex[i2][154] + ex[i2][155]
            if i1 == 160:
                for i2 in ex.columns[d-1:]:
                    ex[i2][160] = ex[i2][161] + ex[i2][162] + ex[i2][163] + ex[i2][164] + ex[i2][165]
            if i1 == 170:
                for i2 in ex.columns[d-1:]:
                    ex[i2][170] = ex[i2][171] + ex[i2][172]
            if i1 == 190:
                for i2 in ex.columns[d-1:]:
                    ex[i2][190] = ex[i2][191]
    if basa > 2:
        for i1 in range(ex.shape[0]):
            if i1 < 2: continue
            ex.iloc[i1]['year'] = period
        if basa == 3:
            m = ["д0{}".format(i+1) for i in range(9)] + ["д{}".format(i+10) for i in range(11)
        ] + ['-', '-', 'н01', 'н02']
        else:
            m = ["о0{}".format(i+1) for i in range(9)] + ["о{}".format(i+10) for i in range(32)
    ] + ['-', '-'] + ["н0{}".format(i+3) for i in range(7)]
        ex.loc[-20] = ["-" for i in range(8)] + m
        ex = ex.sort_index()
    if basa == 3:
        log15.close()
    return ex
def df_tonfp_iar(df, quarter, year):
    if quarter == 4:
        period = str(year)
    else:
        period = "{0}-{1}м".format(year, quarter*3)
    ex3 = db_toexcel(df, quarter, year, 1)
    ex4 = db_toexcel(df, quarter, year, 2)
    ex3_iar = db_toexcel(df, quarter, year, 3)
    ex4_iar = db_toexcel(df, quarter, year, 4)
    filename1 = "csv/{0}-{1}-Розділ_3.xlsx".format(random.randint(1000, 9999), period)
    filename2 = "csv/{0}-{1}-Розділ_4.xlsx".format(random.randint(1000, 9999), period)
    filename3 = "csv/{0}-{1}-ІАР.xlsx".format(random.randint(1000, 9999), period)
    with pd.ExcelWriter("apps/neww/static/"+filename1) as writer:
        ex3.to_excel(writer, index = False, header = False)
    with pd.ExcelWriter("apps/neww/static/"+filename2) as writer:
        ex4.to_excel(writer, index = False, header = False)
    with pd.ExcelWriter("apps/neww/static/"+filename3) as writer:
        pd.DataFrame([]).to_excel(writer, sheet_name='р1', index = False, header = False)
        ex3_iar.to_excel(writer, sheet_name='р3', index = False, header = False)
        ex4_iar.to_excel(writer, sheet_name='р4', index = False, header = False)
        pd.DataFrame([]).to_excel(writer, sheet_name='виплати', index = False, header = False)
        pd.DataFrame([]).to_excel(writer, sheet_name='РЗ', index = False, header = False)
    return filename1, filename2, filename3