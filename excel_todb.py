from . import db
import xlrd
import pandas as pd
import datetime
from bs4 import BeautifulSoup
import lxml
from lxml import etree, html
import requests
import logging
logging.basicConfig(filename="excel_todb.log", level=logging.ERROR)
log = logging.getLogger('0')
user_errors = {}



def pars_todb(data, user_id):
    [url, login, password, quarter, year, c_id] = data
    headers = {'UserName': login, 'Password': password}
    r = requests.post(url, headers=headers)
    html = r.text
    soup = BeautifulSoup(html, 'lxml')
    df = pd.DataFrame([[0]])
    table = soup.find_all('table')
    width = []
    height = []
    n_table = 0
    for table_i in table:
        j = 0
        n_table += 1
        number = False
        dop_w = 0
        if n_table > 1:
            if int(table_i.find_all('tr')[2].find_all('td')[1].next_element) > max(height):
                j = len(height)
            else:
                for i in height:
                    j += 1
                    if i == int(table_i.find_all('tr')[3].find_all('td')[1].next_element):
                        break
            if int(table_i.find_all('th')[int(len(table_i.find_all('th')) / 2)].next_element) > max(width):
                dop_w += len(width)
        else:
            j += 1
        if j == 1:
            i = 0
            th = table_i.find_all('th')
            for th_i in th:
                i += 1
                if i == len(th) / 2 + 1:
                    i = 1
                    j += 1
                    number = True
                if number == True:
                    c = False
                    if width == []:
                        width.append(int(th_i.next_element))
                        c = True
                    else:
                        for k in width:
                            if k == int(th_i.next_element):
                                c = True
                                break
                    if c == False:
                        width.append(int(th_i.next_element))
                if i + dop_w == df.shape[0]:
                    df.loc[i + dop_w] = 0
                if j - 1 == df.shape[1]:
                    df[j - 1] = 0
                df[j - 1][i + dop_w] = th_i.next_element
        tr = table_i.find_all('tr')
        for tr_i in tr:
            j += 1
            td = tr_i.find_all('td')
            if td == []:
                j -= 1
            i = 0
            for a in td:
                i += 1
                if i == 2 and height == []:
                    height.append(int(a.next_element))
                elif i == 2:
                    c = True
                    for x in height:
                        if int(a.next_element) == x:
                            c = False
                            break
                    if c == True:
                        height.append(int(a.next_element))
                if i + dop_w == df.shape[0]:
                    df.loc[i + dop_w] = 0
                if j - 1 == df.shape[1]:
                    df[j - 1] = 0
                df[j - 1][i + dop_w] = a.next_element
    df = df.loc[4:]
    for i in range(df.shape[0]):
        m = {}
        for j in range(df.shape[1]):
            if j == 0:
                m.update({db.type.fields[1]: df[j][i + 4]})
            elif j == 1:
                pass
            else:
                m.update({db.type.fields[j]: df[j][i + 4]})
        c = True
        for i in db(db.type.name == m['name']).select():
            if i.quarter == quarter and i.year == year and i.company_id == m['company_id']:
                i.update_record(**m)
                c = False
                break
        if c is True:
            db.type.insert(**m)
    file = open('text1234.txt', 'w')
    for i in range(df.shape[0]):
        for j in range(df.shape[1]):
            file.write(str(df[j][i]) + '  ')
        file.write('\n')
    file.close()


def join_xml(df):
    ekp = pd.unique(df['ekp'])
    key_s = ['z220', 'k030', 'h015']
    b_s = []
    for k in ekp:
        h011 = pd.unique(df[df['ekp'] == k]['h011'])
        for j in h011:
            df2 = df[df['ekp'] == k][df['h011'] == j]
            names = [df2.iloc[i].name for i in range(df2.shape[0])]
            w_s = []
            for i in names:
                if i in w_s or i in b_s:
                    continue
                for key in range(3):
                    d = df.loc[df['ekp'] == k][df['h011'] == j][df[key_s[key - 1]] == df.loc[i][key_s[key - 1]]][
                        df[key_s[key - 2]] == df.loc[i][key_s[key - 2]]][
                        df[key_s[key - 3]] != df.loc[i][key_s[key - 3]]]
                    if d.shape[0] > 0:
                        names_d = [d.iloc[i_d].name for i_d in range(d.shape[0])]
                        for i1 in names_d:
                            if i1 in w_s or i1 in b_s: continue
                            if d[key_s[key - 3]][i1] == '#':
                                if df[key_s[key - 3]][i] == '#' and i < i1:
                                    b_s.append(i1)
                                elif df[key_s[key - 3]][i] == '#' and i > i1:
                                    b_s.append(i)
                                else:
                                    w_s.append(i)
                            else:
                                if df[key_s[key - 3]][i] == '#':
                                    w_s.append(i1)
                                else:
                                    m = {'ekp': k, 'h011': j}
                                    m['t100'] = df['t100'][i] + d['t100'][i1]
                                    m[key_s[key - 3]] = '#'
                                    m[key_s[key - 2]] = d[key_s[key - 2]][i1]
                                    m[key_s[key - 1]] = d[key_s[key - 1]][i1]
                                    names.append(df.shape[0])
                                    df.loc[df.shape[0]] = m
                                    if i not in w_s: w_s.append(i)
                                    if i1 not in w_s: w_s.append(i1)
            for r in w_s:
                names.remove(r)
            for r in b_s:
                names.remove(r)
    for i in b_s:
        df = df.drop(i)
    return df

def parseXML(data, user_id):
    log11 = open('log11.txt', 'w')
    [xml, k] = data
    user_errors[user_id] = 2
    error = "OK"
    df = []
    df_c = []
    xmlschema_doc = etree.parse("apps/neww/static/tir4.xsd")
    xmlschema = etree.XMLSchema(xmlschema_doc)
    xml_doc = etree.parse(xml)
    try:
        xmlschema.assertValid(xml_doc)
        with open(xml, 'r') as file:
            root = etree.fromstringlist(file, parser=html.HTMLParser(encoding='utf-8'))
    except UnicodeDecodeError:
        error = "Неподходящий формат. Загрузите файл с росширением .xml"
        user_errors[user_id] = 1
    except etree.XMLSyntaxError:
        error = "Неподходящий формат. Загрузите файл с росширением .xml"
        user_errors[user_id] = 1
    except etree.DocumentInvalid:
        error = "Файл не прошел проверку. Свертесь с правилами состовления файла"
    if error == "OK":
        cols = ['ekp', 'h011', 'h015', 'k030', 'z220', 't100']
        df = pd.DataFrame(columns=cols)
        a = {}
        [a.update({i: 0}) for i in cols]
        for h1 in root.getchildren():
            log11.write("1-{}\n".format(h1.tag))
            if h1.tag == "reportdate":
                date = h1.text
            m = {}
            for h2 in h1.getchildren():
                log11.write("2-{}\n".format(h2.tag))
                if h2.tag == "reportdate":
                    date = h2.text
                if h2.tag in cols:
                    m[h2.tag] = h2.text
                else:
                    m = {}
                    for h3 in h2.getchildren():
                        log11.write("3-{}\n".format(h3.tag))
                        if h3.tag == "reportdate":
                            date = h3.text
                        if h3.tag in cols:
                            m[h3.tag] = h3.text
                        else:
                            m = {}
                            for h4 in h3.getchildren():
                                log11.write("4-{}\n".format(h4.tag))
                                if h4.tag == "reportdate":
                                    date = h4.text
                                if h4.tag in cols:
                                    m[h4.tag] = h4.text
                                else:
                                    m = {}
                                    for h5 in h4.getchildren():
                                        log11.write("5-{}\n".format(h5.tag))
                                        if h5.tag in cols:
                                            m[h5.tag] = h5.text
                                    if m != {}:
                                        log11.write("-{}-\n".format(m['t100']))
                                        m['t100'] = float(m['t100']) * k
                                        df.loc[df.shape[0]] = m
                                        m = {}
                            if m != {}:
                                m['t100'] = float(m['t100']) * k
                                df.loc[df.shape[0]] = m
                                m = {}
                    if m != {}:
                        m['t100'] = float(m['t100']) * k
                        df.loc[df.shape[0]] = m
                        m = {}
            if m != {}:
                m['t100'] = float(m['t100']) * k
                df.loc[df.shape[0]] = m
    quarter, year = pd.to_datetime(date, dayfirst=True).quarter, pd.to_datetime(date).year
    if error == "OK":
        df_c = df.copy()
        for i1 in range(df.shape[0]):
            df_c.iloc[i1] = a
    result = [error, df, df_c, quarter, year]
    log11.close()
    return result

def df_todb_ir4(data):
    if len(data) == 4:
        [df, quarter, year, company_id] = data
    else:
        [df, company_id] = data
    for i1 in range(df.shape[0]):
        m = {}
        if len(data) == 4:
            m['quarter'] = quarter
            m['year'] = year
        m['company_id'] = company_id
        for i2 in df.columns:
            m[i2] = str(df.iloc[i1][i2])
        m['t100'] = float(m['t100'])
        c = True
        querry = (db.type.id > 0) & (db.type.h011 == m['h011']) & (db.type.ekp == m['ekp']) & (
                    db.type.company_id == str(m['company_id'])) \
                 & (db.type.h015 == m['h015']) & (db.type.k030 == m['k030']) & (db.type.z220 == m['z220']) \
                 & (db.type.year == str(m['year'])) & (db.type.quarter == str(m['quarter']))
        for i in db(querry).select():
            i.update_record(**m)
            c = False
            break
        if c is True:
            db.type.insert(**m)

def df_todb_payout(df, company_id, key):
    for i1 in range(df.shape[0]):
        if i1 == 0: continue
        c = True
        m = {}
        for i2 in range(df.shape[1]):
            m[db.payout.fields[i2 + 1]] = str(df[df.columns[i2]][i1])
        m['company_id'] = company_id
        try:
            if key == 1:
                m['insurance_type'] = db(db.vid_strah.iar_kod == df['insurance_type'][i1]).select()[0].id
        except IndexError:
            pass
        else:
            querry = (db.payout.company_id == str(m['company_id'])) & (db.payout.statement_date == m['statement_date']) & (
                db.payout.case_num == m['case_num']) & (db.payout.insurance_type == m['insurance_type'])
            try:
                for i in db(querry).select():
                    c = False
                    break
            except ValueError:
                pass
            if c is True:
                db.payout.insert(**m)

def df_todb_rezerv(df, company_id, key):
    for i1 in range(df.shape[0]):
        c = True
        if i1 == 0: continue
        m = {}
        for i2 in range(df.shape[1]):
            m[db.rezerv.fields[i2 + 1]] = str(df[df.columns[i2]][i1])
        m['company_id'] = company_id
        try:
            if key == 1:
                m['insurance_type'] = db(db.vid_strah.iar_kod == df['insurance_type'][i1]).select()[0].id
        except IndexError:
            pass
        else:
            querry = (db.rezerv.company_id == str(m['company_id'])) & (db.rezerv.statement_date == m['statement_date']) & (
                db.rezerv.case_num == m['case_num']) & (db.rezerv.insurance_type == m['insurance_type'])
            try:
                for i in db(querry).select():
                    c = False
                    break
            except ValueError:
                pass
            if c is True:
                db.rezerv.insert(**m)

def df_todb_nfp(data):
    if len(data) == 4:
        [df, quarter, year, company_id] = data
    else:
        [df, company_id] = data
    v38 = 0
    for i1 in range(df.shape[0]):
        if i1 < 2: continue
        row = db(db.ir4_description.p_id == df[1][i1]).select()[0]
        for i2 in range(df.shape[1]):
            if i2 < 3: continue
            if df[i2][i1] != 0:
                m = {'ekp': str(row.ekp), 'h011': str(df[i2][1]), 'h015': str(row.h015),
                     'k030': str(row.k030), 'z220': str(row.z220), 'quarter': quarter,
                     'year': year, 'company_id': company_id}
                if int(df[i2][1]) == 38:
                    if int(df[i2 + 1][1]) == 38:
                        v38 = int(df[i2][i1]) + int(df[i2 + 1][i1])
                    else:
                        v38 = int(df[i2][i1])
                    m['t100'] = str(v38)
                    v38 = 1
                elif v38 == 1:
                    pass
                else:
                    m['t100'] = str(df[i2][i1])
                c = True
                querry = (db.type.id > 0) & (db.type.h011 == m['h011']) & (db.type.ekp == m['ekp']) & (
                            db.type.company_id == str(m['company_id'])) \
                         & (db.type.h015 == m['h015']) & (db.type.k030 == m['k030']) & (db.type.z220 == m['z220']) \
                         & (db.type.year == str(m['year'])) & (db.type.quarter == str(m['quarter']))
                for i in db(querry).select():
                    c = False
                    break
                if c is True:
                    db.type.insert(**m)

def df_todb_iar(df, company_id):
    for i1 in range(df.shape[0]):
        if i1 < 1: continue
        for i2 in df.columns:
            if i2 == 'p_id':
                row = db(db.ir4_description.p_id == int(df[i2][i1])).select()[0]
            elif i2 == 'year' or i2 == 'quarter':
                pass
            else:
                if df[i2][i1] != 0:
                    m = {'ekp': str(row.ekp), 'h015': str(row.h015), 'k030': str(row.k030), 
                    'z220': str(row.z220), 'quarter': df['quarter'][i1], 'year': df['year'][i1], 
                    'company_id': company_id, 't100': df[i2][i1]}
                    m['h011'] = db(db.vid_strah.iar_kod == i2).select()[0].id
                    c = True
                    querry = (db.type.id > 0) & (db.type.h011 == m['h011']) & (db.type.ekp == m['ekp']) & (
                                db.type.company_id == str(m['company_id'])) \
                            & (db.type.h015 == m['h015']) & (db.type.k030 == m['k030']) & (db.type.z220 == m['z220']) \
                            & (db.type.year == str(m['year'])) & (db.type.quarter == str(m['quarter']))
                    for i in db(querry).select():
                        c = False
                        break
                    if c is True:
                        db.type.insert(**m)

def df_todb(result, user_id):
    user_errors[user_id] = 10
    log222 = open('log222.txt', 'w')
    if len(result) == 5:
        df = result[0]
        quarter = result[1]
        year = result[2]
        company_id = result[3]
        table = result[4]
    else:
        company_id = result[1]
        table = result[2]
        if table == 6:
            df1 = result[0][0][1]
            df2 = result[0][1][1]
            df3 = result[0][2][1]
            df4 = result[0][3][1]
        else:
            df = result[0]
    if table == 3:
        df_todb_payout(df, company_id, 0)
    elif table == 4:
        df_todb_rezerv(df, company_id, 0)
    elif table == 5:
        df_todb_ir4([df, quarter, year, company_id])
        error = True
        if quarter == 1:
            pass
        else:
            log222.write('{0}--{1}--{2}\n'.format(quarter, year, company_id))
            data = db((db.type.quarter == quarter-1) & (db.type.year == year) & (db.type.company_id == company_id)).select()
            if len(data) > 0:
                df_data = pd.DataFrame(columns = ['ekp', 'h011', 'h015', 'k030', 'z220', 't100'])
                for j in data:
                    df_data.loc[df_data.shape[0]] = [j.ekp, j.h011, j.h015, j.k030, j.z220, j.t100]
                for i1 in range(df.shape[0]):
                    d = df_data[df_data['ekp'] == df.iloc[i1]['ekp']][df_data['h011'] == df.iloc[i1]['h011']][df_data['h015'
                    ] == df.iloc[i1]['h015']][df_data['k030'] == df.iloc[i1]['k030']][df_data['z220'] == df.iloc[i1]['z220']]
                    if len(d) > 0:
                        df.iloc[i1] -= d['t100']
            else:
                error = False
        if error:
            for j in range(df.shape[0]):
                m = {'quarter': quarter, 'year': year, 'company_id': company_id}
                for k in df.columns:
                    m[k] = df.iloc[j][k]
                c = True
                querry = (db.type_orig.id > 0) & (db.type_orig.h011 == m['h011']) & (db.type_orig.ekp == m['ekp']) & (
                            db.type_orig.company_id == str(m['company_id'])) \
                        & (db.type_orig.h015 == m['h015']) & (db.type_orig.k030 == m['k030']) & (db.type_orig.z220 == m['z220']) \
                        & (db.type_orig.year == str(m['year'])) & (db.type_orig.quarter == str(m['quarter']))
                for i in db(querry).select():
                    c = False
                    break
                if c:
                    try:
                        db.type_orig.insert(**m)
                    except KeyError:
                        db.type_orig.update_record(**m)
    elif table == 6:
        df_todb_iar(df1, company_id)
        df_todb_iar(df2, company_id)
        df_todb_payout(df3, company_id, 1)
        df_todb_rezerv(df4, company_id, 1)
    else:
        df_todb_nfp([df, quarter, year, company_id])
    log222.close()
    return "Операция успешно завершена"


def db34(df, k, user_id):
    error = "OK"
    user_errors[user_id] = 9
    df_c = df.copy()
    for i1 in range(df.shape[0]):
        for i2 in range(df.shape[1]):
            df_c[i2][i1] = 0
            if i2 < 2 or i1 < 2:
                pass
            else:
                a = str(df[i2][i1])
                for i in range(len(a)):
                    try:
                        if a[i] == ' ':
                            a = a[:i] + a[i + 1:]
                            df_c[i2][i1] = 1
                    except IndexError:
                        pass
                try:
                    a = int(float(a) * 10 * k) / 10
                    df[i2][i1] = a
                except ValueError:
                    df_c[i2][i1] = 2
                    error = "OK Возникли проблемы с некоторыми ячейками (несоответсвующий формат данных)"
    return [error, df, df_c]


def payout_todb(df, table, k, user_id):
    log444 = open('log444.txt', 'w')
    user_errors[user_id] = 9
    error = "OK"
    for i in df.columns:
        if df[i].notnull().sum() == 0:
            del df[i]
    for i in range(df.shape[0]):
        if df.loc[i].isnull().sum() > 0:
            df.drop([i], inplace=True)
    if table == 3:
        f = xlrd.open_workbook('apps/neww/static/Журнал виплат.xlsx')
    else:
        f = xlrd.open_workbook('apps/neww/static/Резерв заявлених збитків.xlsx')
    ideal_page = f.sheet_by_index(0)
    c = True
    for i1 in range(9):
        if df.iloc[0][i1] != ideal_page.cell_value(1, i1):
            c = False
            break
    if c:
        if table == 3 and df.iloc[0][9] == ideal_page.cell_value(1, 9):
            if df.shape[1] == 11:
                del df[10]
        elif table == 4 and df.iloc[0][9] == ideal_page.cell_value(1, 9) and df.iloc[0][10] == ideal_page.cell_value(1,
                                                                                                                     10):
            pass
        else:
            c = False
    if c:
        df.columns = [i for i in range(df.shape[1])]
        df_c = df.copy()
        for i1 in range(df.shape[0]):
            for i2 in range(df.shape[1]):
                df_c[i2][i1] = 0
        for i1 in range(df.shape[0]):
            if i1 == 0: continue
            for i2 in range(6):
                if i2 == 5 and table == 3:
                    continue
                elif i2 == 5 and table == 4:
                    i2 = 7
                if type(df[i2 + 3][i1]) != type(datetime.datetime(2000, 1, 1)):
                    a = str(df[i2 + 3][i1])
                    for i in range(len(a)):
                        if a[i] in ' . /_':
                            a = a[:i] + '-' + a[i + 1:]
                            df_c[i2 + 3][i1] = 1
                    log444.write('{0}    {1}\n'.format(a, table))
                    if len(a) == 10 and len(a[:a.index('-')]) == 2:
                        a = pd.to_datetime(a, dayfirst=True)
                    elif len(a) < 2:
                        a = None
                    elif len(a) < 10:
                        if a.index('-') < 2:
                            a = '0' + a
                        c = a[:2]
                        a = a[3:]
                        if a.index('-') < 2:
                            a = '0' + a
                        d = a[:2]
                        a = a[3:]
                        if len(a) < 4:
                            a = '20' + a
                        a = a + '-' + d + '-' + c
                    elif len(a) > 10:
                        df_c = 2
                    try:
                        a = pd.to_datetime(a, dayfirst=True)
                    except pd._libs.tslibs.parsing.DateParseError:
                        pass
                    df[i2 + 3][i1] = a
                else:
                    df[i2 + 3][i1] = datetime.date(df[i2 + 3][i1].year, df[i2 + 3][i1].month, df[i2 + 3][i1].day)
        for i1 in range(df.shape[0]):
            if i1 == 0: continue
            for i2 in df.columns:
                if (i2 > 7 and i2 < 11 and table == 3) or (i2 > 7 and i2 < 10 and table == 4):
                    a = str(df[i2][i1])
                    for i in range(len(a)):
                        try:
                            if a[i] in ' .,':
                                a = a[:i] + a[i + 1:]
                                df[i2][i1] = 1
                        except IndexError:
                            pass
                    try:
                        a = int(float(a) * 10 * k) / 10
                        df[i2][i1] = a
                    except ValueError:
                        df_c[i2][i1] = 2
                        error = "OK Возникли проблемы с некоторыми ячейками (несоответсвующий формат данных)"
                elif i2 == 0:
                    a = str(df[i2][i1]).lower()
                    c = 0
                    for i in db(db.vid_strah).select():
                        m1 = [i.key1, i.key2, i.key3]
                        c = 0
                        for j in m1:
                            log444.write('{}\n'.format(j))
                            if (j in a) == False:
                                c = False
                                break
                            elif j == None:
                                break
                        if c == 0:
                            c = i.id
                            break
                    log444.write('{}\n'.format(c))
                    if c > 0:
                        a = db(db.vid_strah.id == c).select()[0].nbu_id
                    else:
                        df_c[i2][i1] = 2
                    df[i2][i1] = a
                    log444.write('vid opredelen\n')
    else:
        error = "Не співпадає з прикладом. Використовуйте його як бланк"
        df = []
    log444.write('THE_END!!!!!')
    log444.close()
    return error, df, df_c

def iar_todf(xl, user_id):
    error = "OK"
    result = []
    user_errors[user_id] = 3
    try:
        f = xlrd.open_workbook(xl)
    except xlrd.biffh.XLRDError:
        error = "Неподходящий формат. Загрузите файл с росширением .xls или .xlsx"
    if error == "OK":
        user_errors[user_id] = 4
        page = f.sheet_by_name('р3')
        cols = ['p_id']
        for i in range(21):
            if i < 9:
                cols.append('д0'+str(i+1))
            else:
                cols.append('д'+str(i+1))
        cols.append('н01')
        cols.append('н02')
        cols.append('year')
        cols.append('quarter')
        df = pd.DataFrame(columns = [i for i in range(page.ncols)])
        for i1 in range(page.nrows):
            if i1 == 0 or i1 == 2: continue
            c = False
            for i2 in range(page.ncols-8):
                if page.cell_value(i1, i2+8) in ['', ' ', 'нд', 'х', None]:
                    pass
                else:
                    c = True
                    break
            if c is True:
                m = {}
                for i2 in range(page.ncols):
                    m[i2] = page.cell_value(i1, i2)
                    if m[i2] in ['', ' ', 'нд', 'х', None]:
                        m[i2] = 0
                    if i2 > 7:
                        try:
                            m[i2] = int(m[i2]*100000)
                        except ValueError:
                            pass
                df.loc[df.shape[0]] = m
        del df[0]
        del df[1]
        del df[2]
        del df[5]
        del df[6]
        del df[7]
        del df[29]
        df[32] = df[4]
        del df[4]
        df[50] = [None for i in range(df.shape[0])]
        df.columns = cols
        for i in range(df.shape[0]):
            if i == 0:
                df['year'][i] = 'рік'
                df['quarter'][i] = 'квартал'
                continue
            a = str(df.loc[i]['year'])
            df['quarter'][i] = int(int(a[5]) / 3)
            if df['quarter'][i] == 0:
                df['quarter'][i] = 4
            df['year'][i] = int(a[:4])
        result.append(["OK", df])
        user_errors[user_id] = 5
        page = f.sheet_by_name('р4')
        cols = ['p_id']
        for i in range(42):
            if i < 9:
                cols.append('о0'+str(i+1))
            else:
                cols.append('о'+str(i+1))
        for i in range(7):
            cols.append('н0'+str(i+3))
        cols.append('year')
        cols.append('quarter')
        df = pd.DataFrame(columns = [i for i in range(page.ncols)])
        for i1 in range(page.nrows):
            if i1 == 0 or i1 == 2: continue
            c = False
            for i2 in range(page.ncols-8):
                if page.cell_value(i1, i2+8) in ['', ' ', 'нд', 'х', None]:
                    pass
                else:
                    c = True
                    break
            if c is True:
                m = {}
                for i2 in range(page.ncols):
                    m[i2] = page.cell_value(i1, i2)
                    if m[i2] in ['', ' ', 'нд', 'х', None]:
                        m[i2] = 0
                    if i2 > 7:
                        try:
                            m[i2] = int(m[i2]*100000)
                        except ValueError:
                            pass
                df.loc[df.shape[0]] = m
        del df[0]
        del df[1]
        del df[2]
        del df[5]
        del df[6]
        del df[7]
        del df[50]
        df[79] = df[4]
        del df[4]
        df[80] = [None for i in range(df.shape[0])]
        df.columns = cols
        for i in range(df.shape[0]):
            if i == 0:
                df['year'][i] = 'рік'
                df['quarter'][i] = 'квартал'
                continue
            a = str(df.loc[i]['year'])
            df['quarter'][i] = int(int(a[5]) / 3)
            if df['quarter'][i] == 0:
                df['quarter'][i] = 4
            df['year'][i] = int(a[:4])
        result.append(["OK", df])
        user_errors[user_id] = 6
        page = f.sheet_by_name('виплати')
        cols = db.payout.fields[1:-1]
        df = pd.DataFrame(columns = cols)
        f_p = xlrd.open_workbook('apps/neww/static/Журнал виплат.xlsx')
        df.loc[0] = f_p.sheet_by_index(0).row_values(1)[:len(db.payout.fields[1:-1])]
        for i1 in range(page.nrows):
            if i1 < 2: continue
            m = {cols[0]: str(page.cell_value(i1, 12)), cols[1]: page.cell_value(i1, 1),
                cols[2]: page.cell_value(i1, 2), cols[5]: None,
                cols[9]: page.cell_value(i1, 11)}
            m[cols[8]] = page.cell_value(i1, 5)
            if m[cols[8]] == '':
                m[cols[8]] = page.cell_value(i1, 10)
            try:
                m[cols[3]] = xlrd.xldate_as_datetime(int(page.cell_value(i1, 3)), 0).date()
            except ValueError:
                m[cols[3]] = None
            try:
                m[cols[4]] = xlrd.xldate_as_datetime(int(page.cell_value(i1, 4)), 0).date()
            except ValueError:
                m[cols[4]] = None
            try:
                m[cols[6]] = xlrd.xldate_as_datetime(int(page.cell_value(i1, 6)), 0).date()
            except ValueError:
                m[cols[6]] = None
            try:
                m[cols[7]] = xlrd.xldate_as_datetime(int(page.cell_value(i1, 9)), 0).date()
            except ValueError:
                m[cols[7]] = None
            for i in m.keys():
                if m[i] == '':
                    m[i] = '-'
            df.loc[df.shape[0]] = m
        result.append(["OK", df])
        user_errors[user_id] = 7
        page = f.sheet_by_name('РЗ')
        cols = db.rezerv.fields[1:-1]
        df = pd.DataFrame(columns = cols)
        f_r = xlrd.open_workbook('apps/neww/static/Резерв заявлених збитків.xlsx')
        df.loc[0] = f_r.sheet_by_index(0).row_values(1)[:len(db.rezerv.fields[1:-1])]
        for i1 in range(page.nrows):
            if i1 < 2: continue
            m = {cols[0]: str(page.cell_value(i1, 10)), cols[1]: page.cell_value(i1, 1),
                cols[2]: page.cell_value(i1, 2), cols[5]: '-', cols[6]: '-', cols[7]: '-',
                cols[8]: page.cell_value(i1, 5), cols[9]: page.cell_value(i1, 6)}
            try:
                m[cols[3]] = xlrd.xldate_as_datetime(int(page.cell_value(i1, 3)), 0).date()
            except ValueError:
                m[cols[3]] = None
            try:
                m[cols[4]] = xlrd.xldate_as_datetime(int(page.cell_value(i1, 4)), 0).date()
            except ValueError:
                m[cols[4]] = None
            try:
                m[cols[10]] = xlrd.xldate_as_datetime(int(page.cell_value(i1, 9)), 0).date()
            except ValueError:
                m[cols[10]] = None
            for i in m.keys():
                if m[i] == '':
                    m[i] = '-'
            df.loc[df.shape[0]] = m
        result.append(["OK", df])
    else:
        result = [[error, []]]
    return result

def import_excel(data, user_id):
    [xl, table, k] = data
    log_im = open('log_im.txt', 'w')
    error = "OK"
    result = []
    user_errors[user_id] = 3
    try:
        f = xlrd.open_workbook(xl)
    except xlrd.biffh.XLRDError:
        error = "Неподходящий формат. Загрузите файл с росширением .xls или .xlsx"
    if error == "OK":
        page = f.sheet_by_index(0)
        if table > 2:
            df = pd.read_excel(xl)
            result = payout_todb(df, table, k, user_id)
        else:
            if table == 1:
                a = 100
                file = 'apps/neww/static/Раздел 3.xlsx'
            else:
                a = 200
                file = 'apps/neww/static/Раздел 4.xlsx'
            f = xlrd.open_workbook(file)
            c = True
            for sheet in range(f.nsheets):
                ideal_page = f.sheet_by_index(sheet)
                if ideal_page.nrows == page.nrows:
                    c = True
                    break
                else:
                    c = False
            if c is True:
                for i1 in range(7):
                    if c is False: break
                    for i2 in range(page.ncols):
                        if page.cell_value(i1, i2) != ideal_page.cell_value(i1, i2):
                            c = False
                            break
                for i1 in range(page.nrows):
                    if c is False: break
                    for i2 in range(2):
                        if page.cell_value(i1, i2) != ideal_page.cell_value(i1, i2):
                            c = False
                            break
            if c is False:
                user_errors[user_id] = 8
                result = ["Не співпадає з прикладом. Використовуйте його як бланк", [], []]
            else:
                t = []
                for i1 in range(page.nrows):
                    if i1 < 5: continue
                    if page.nrows > 80 and (i1 == 39 or i1 == 40 or i1 == 57 or i1 == 58):
                        continue
                    t.append([])
                    for i2 in range(page.ncols):
                        if i1 == 6:
                            if i2 == 0:
                                t[-1].append('Код за НБУ')
                            elif i2 < 3:
                                t[-1].append('-')
                            else:
                                log_im.write(str(i2 - 2))
                                t[-1].append(db(db.vid_strah.nfp_id == str(i2 - 2 + a)).select()[0].id)
                        else:
                            t[-1].append(page.cell_value(i1, i2))
                df = pd.DataFrame(t)
                result = db34(df, k, user_id)
    else:
        result = [error, [], []]
    log_im.close()
    return result

def main(key, data, user_id):
    try:
        if key == 1:
            f = 0
            f = pars_todb(data, user_id)
        elif key == 2:
            f = ["Неизвестная ошибка", [], []]
            f = parseXML(data, user_id)
        elif key == 3:
            f = ["Неизвестная ошибка"]
            f = df_todb(data, user_id)
        elif key == 4:
            f = ["Неизвестная ошибка", [], []]
            f = import_excel(data, user_id)
        else:
            f = [["Неизвестная ошибка", []]]
            f = iar_todf(data, user_id)
    except AttributeError:
        log.exception(str(datetime.datetime.today().time()))
    except UnicodeDecodeError:
        log.exception(str(datetime.datetime.today().time()))
    except IndexError:
        log.exception(str(datetime.datetime.today().time()))
    except NameError:
        log.exception(str(datetime.datetime.today().time()))
    except ValueError:
        log.exception(str(datetime.datetime.today().time()))
    except RuntimeError:
        log.exception(str(datetime.datetime.today().time()))
    except TypeError:
        log.exception(str(datetime.datetime.today().time()))
    except KeyError:
        log.exception(str(datetime.datetime.today().time()))
    except UnicodeError:
        log.exception(str(datetime.datetime.today().time()))
    except UnicodeEncodeError:
        log.exception(str(datetime.datetime.today().time()))
    except ZeroDivisionError:
        log.exception(str(datetime.datetime.today().time()))
    except FileNotFoundError:
        log.exception(str(datetime.datetime.today().time()))
    except KeyError:
        log.exception(str(datetime.datetime.today().time()))
    except lxml.etree.XMLSyntaxError:
        log.exception(str(datetime.datetime.today().time()))
    except UnboundLocalError:
        log.exception(str(datetime.datetime.today().time()))
    else:
        key = 10
    if key == 1:
        f = 0
    elif key == 2:
        f = [user_errors[user_id], [], [], [], []]
    elif key == 4:
        f = [user_errors[user_id], [], []]
    elif key == 3:
        f = [user_errors[user_id]]
    elif key == 5:
        f = [[user_errors[user_id], []]]
    return f, user_errors[user_id]
