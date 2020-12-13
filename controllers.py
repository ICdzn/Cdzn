"""
This file defines actions, i.e. functions the URLs are mapped into
The @action(path) decorator exposed the function at URL:

    http://127.0.0.1:8000/{app_name}/{path}

If app_name == '_default' then simply

    http://127.0.0.1:8000/{path}

If path == 'index' it can be omitted:

    http://127.0.0.1:8000/

The path follows the bottlepy syntax.

@action.uses('generic.html')  indicates that the action uses the generic.html template
@action.uses(session)         indicates that the action uses the session
@action.uses(db)              indicates that the action uses the db
@action.uses(T)               indicates that the action uses the i18n & pluralization
@action.uses(auth.user)       indicates that the action requires a logged in user
@action.uses(auth)            indicates that the action requires the auth object

session, db, T, auth, and tempates are examples of Fixtures.
Warning: Fixtures MUST be declared with @action.uses({fixtures}) else your app will result in undefined behavior
"""

from py4web import action, request, abort, redirect, URL, Field
from yatl.helpers import A
from .common import db, session, T, cache, auth, logger, authenticated, unauthenticated
from py4web.utils.form import Form, FormStyleBulma
from pydal.validators import IS_IN_SET
from . import excel_todb, server_func
import datetime, random, xlrd
import pandas as pd
from lxml import etree, html
users_block = {}
for i in db(db.company_user).select():
    users_block[i.site_user] = 0
if len(db(db.ir2_description).select()) < 1:
    for i in range(91):
        if i < 9:
            db.ir2_description.insert(p_id=i+1, ekp="IR2000"+str(i+1))
        else:
            db.ir2_description.insert(p_id=i+1, ekp="IR200"+str(i+1))

def func():
    df2 = pd.read_excel('ir4_description.xls').drop(columns=['Unnamed: 0'])
    for i in range(df2.shape[0]):
        m = {}
        for j in df2.columns:
            m[j] = df2[j][i]
            if str(m[j]) == 'nan':
                m[j] = None
        db.ir4_description.insert(**m)

@authenticated()
@action.uses(auth)
def index():
    user = auth.get_user()
    log1 = open('log1.txt', 'w')
    if len(db(db.ir4_description).select()) < 1:
        func()
    log1.close()
    log = open('history.log', 'a')
    log.write("User {0} ({1} {2}) in system at {3}\n".format(user['id'], user['first_name'], user['last_name'], datetime.datetime.today().time()))
    log.close()
    message = T("Hello {first_name}".format(**user))
    return dict(message=message, user=user)


@action("pars",method="GET")
@action.uses("pars_from.html", auth)
def pars_get():
    user = auth.get_user()
    c = []
    b = []
    for i in db(db.company_user).select():
        if i.site_user == user['id']:
            j = db(db.company.id == i.company_id).select()[0]
            c.append(j.IAN_FULL_NAME)
            b.append(j.id)
    return dict(message="",new_data_dict={},session=session,companies=c, comp=b, user=user)


@action("static/pars",method="POST")
@action.uses("confirm.html", db, auth)
def pars_post():
    user = auth.get_user()
    quarter=request.POST['quarter']
    year=request.POST['year']
    c=request.POST['company']
    url=request.POST['URL']
    login=request.POST['Login']
    password=request.POST['Password']
    for i in db(db.company).select():
        if i.id == c:
            message, error_kod = excel_todb.main(1, [url, login, password, int(quarter), int(year), i.id], user['id'])
            error = db(db.errors.kod == error_kod).select()[0].ua
    return dict(message=message, error=error, user=user)

@action("download",method="GET")
@action.uses("download.html", auth)
def download_get():
    c = []
    b = []
    user = auth.get_user()
    if len(user) > 0:
        for i in db(db.company_user).select():
            if i.site_user == user['id'] or user['id'] == 1:
                j = db(db.company.id == i.company_id).select()[0]
                c.append(j.IAN_FULL_NAME)
                b.append(j.id)
    return dict(message="",new_data_dict={},session=session,companies=c, comp=b, user=user)

@action("static/download",method="POST")
@action.uses("result_page.html", auth)
def info_db_post():
    message = ""
    filename = ''
    user = auth.get_user()
    try:
        d1=request.POST['date1']
        d2=request.POST['date2']
        basa=int(request.POST['basa'])
        company_id=request.POST['company']
        df = server_func.show_db(company_id, d1, d2, basa)
        filename = "csv/{}.xlsx".format(random.randint(10000, 99999))
        df.to_excel("apps/neww/static/"+filename, index = False, header = False)
        message = "OK"
    except KeyError:
        df = []
        message = str(random.randint(0, 100))
        excel_todb.log.exception(str(datetime.datetime.today().time()))
    except AttributeError:
        df = []
        message = "Не найдено данных за выбраный период"
        excel_todb.log.exception(str(datetime.datetime.today().time()))
    return dict(session=session, user=user, message=message, url=filename)

@action("multi_upload",method="GET")
@action.uses("multi_up.html", auth)
def multi_up_get():
    user = auth.get_user()
    c = []
    b = []
    for i in db(db.company_user).select():
        if i.site_user == user['id'] or user['id'] == 1:
            j = db(db.company.id == i.company_id).select()[0]
            c.append(j.IAN_FULL_NAME)
            b.append(j.id)
    return dict(user=user, session=session, companies=c, comp=b)

@action("static/multi_upload",method="POST")
@action.uses("result_page.html", auth)
def multi_up_post():
    log444 = open('log444.txt', 'w')
    user = auth.get_user()
    c = request.POST['company']
    ff = request.files.getall('File')
    for i in ff:
        log444.write(str(i.filename)+'\n')
    xmlschema_doc = etree.parse("apps/neww/static/tir4.xsd")
    xmlschema = etree.XMLSchema(xmlschema_doc)
    f_errors = pd.DataFrame(columns=['filename', 'error'])
    files = pd.DataFrame(columns=['filename', 'year', 'quarter', 'prior'])
    for f in ff:
        file = "apps/neww/uploads/{0}-{1}".format(random.randint(0, 10000), f.filename)
        log444.write(str(f.filename)+'\n')
        f.save(file)
        if '.xml' in file:
            xml_doc = etree.parse(file)
            try:
                xmlschema.assertValid(xml_doc)
                with open(file, 'r') as fil:
                    root = etree.fromstringlist(fil, parser=html.HTMLParser(encoding='utf-8'))
            except UnicodeDecodeError:
                f_errors.loc[f_errors.shape[0]] = db(db.errors.kod == 1).select()[0].ua, file
            except etree.XMLSyntaxError:
                f_errors.loc[f_errors.shape[0]] = db(db.errors.kod == 1).select()[0].ua, file
            except etree.DocumentInvalid:
                f_errors.loc[f_errors.shape[0]] = db(db.errors.kod == 2).select()[0].ua, file
            else:
                date = 0
                for h1 in root.getchildren():
                    if h1.tag == "reportdate":
                        date = h1.text
                        break
                    else:
                        for h2 in h1.getchildren():
                            if h2.tag == "reportdate":
                                date = h2.text
                                break
                            else:
                                for h3 in h2.getchildren():
                                    if h3.tag == "reportdate":
                                        date = h3.text
                                        break
                                    else:
                                        for h4 in h3.getchildren():
                                            if h4.tag == "reportdate":
                                                date = h4.text
                                                break
                if date != 0:
                    quarter, year = pd.to_datetime(date).quarter, pd.to_datetime(date).year
                    files.loc[files.shape[0]] = {'filename': file, 'year': year, 'quarter': quarter, 'prior': 3}
        else:
            if 'ІАР' in file or 'IAP' in file or 'IAR' in file:
                files.loc[files.shape[0]] = {'filename': file, 'year': '', 'quarter': '', 'prior': 1}
            else:
                quarter, year = int(f.filename[0]), int(f.filename[2:6])
                if 'R-3' in file or 'R-4' in file:
                    files.loc[files.shape[0]] = {'filename': file, 'year': year, 'quarter': quarter, 'prior': 2}
                elif'payout' in file or 'rezerv' in file:
                    files.loc[files.shape[0]] = {'filename': file, 'year': year, 'quarter': quarter, 'prior': 4}
    files = files.sort_values(by=['prior', 'year', 'quarter'])
    log444.write(str(files))
    db(db.type.company_id == c).delete()
    db(db.type_orig.company_id == c).delete()
    db(db.payout.company_id == c).delete()
    db(db.rezerv.company_id == c).delete()
    for i in range(files.shape[0]):
        text = 'Операция успешно завершена'
        if files.iloc[i]['prior'] == 1:
            result, error_kod = excel_todb.main(5, files.iloc[i]['filename'], user['id'])
            text, error_kod = excel_todb.main(3, [result, db(db.company.id == c).select()[0].id, 6], user['id'])
        elif files.iloc[i]['prior'] == 2:
            if 'R-3' in file:
                result, error_kod = excel_todb.main(4, [files.iloc[i]['filename'], 1, 100000], user['id'])
                text, error_kod = excel_todb.main(3, [result[1], files.iloc[i]['quarter'], files.iloc[i]['year'], c, 1], user['id'])
            else:
                result, error_kod = excel_todb.main(4, [files.iloc[i]['filename'], 1, 100000], user['id'])
                text, error_kod = excel_todb.main(3, [result[1], files.iloc[i]['quarter'], files.iloc[i]['year'], c, 2], user['id'])
        elif files.iloc[i]['prior'] == 3:
            result, error_kod = excel_todb.main(2, [files.iloc[i]['filename'], 1], user['id'])
            text, error_kod = excel_todb.main(3, [result[1], result[3], result[4], db(db.company.id == c).select()[0].id, 5], user['id'])
        else:
            if 'payout' in files.iloc[i]['filename']:
                result, error_kod = excel_todb.main(4, [files.iloc[i]['filename'], 3, 100], user['id'])
                text, error_kod = excel_todb.main(3, [result[1], c, 3], user['id'])
            elif 'rezerv' in files.iloc[i]['filename']:
                result, error_kod = excel_todb.main(4, [files.iloc[i]['filename'], 4, 100], user['id'])
                text, error_kod = excel_todb.main(3, [result[1], c, 4], user['id'])
        if text != 'Операция успешно завершена':
            error = db(db.errors.kod == error_kod).select()[0].ua + '  /// Ошибка произошла в файле {}'.format(files.iloc[i]['filename'])
            break
        else:
            error = 'OK'
        log444.write(str(error)+'\n')
    df = []
    q_min = y_min = 9999
    q_max = y_max = 0
    for i in db(db.type).select():
        if i.year < y_min:
            y_min = i.year
            q_min = i.quarter
        elif i.year > y_max:
            y_max = i.year
            q_max = i.quarter
        elif i.year == y_min and i.quarter < q_min:
            q_min = i.quarter
        elif i.year == y_max and i.quarter > q_max:
            q_max = i.quarter
    for i in range((y_max-y_min)*4+q_max-q_min+1):
        quarter = (q_min + i) % 4
        if quarter == 0:
            quarter = 4
            year = y_min + int((q_min + i) / 4) - 1
        else:
            year = y_min + int((q_min + i) / 4)
        m = {'quarter': quarter, 'year': year}
        m['type'] = False
        if len(db((db.type.company_id == c) & (db.type.year == year) & (db.type.quarter == quarter)).select()) > 0:
            m['type'] = True
        m['payout'] = False
        for z in db((db.payout.company_id == c) & (db.payout.insurance_payment_date != None)).select():
            if z.insurance_payment_date.year == year and z.insurance_payment_date.month in [quarter*3, quarter*3-1, quarter*3-2]:
                m['payout'] = True
                break
        m['rezerv'] = False
        for z in db((db.rezerv.company_id == c) & (db.rezerv.insert_date != None)).select():
            if z.insert_date.year == year and z.insert_date.month in [quarter*3, quarter*3-1, quarter*3-2]:
                m['rezerv'] = True
                break
        df.append(m)
    log444.write(str(df))
    log444.close()
    return dict(message=text, user=user, url='', error=error, df=df)

@action("convert_ir4",method="GET")
@action.uses("convert_ir4.html", auth)
def convert_get():
    user = auth.get_user()
    return dict(user=user, session=session)

@action("static/convert_ir4",method="POST")
@action.uses("convert2.html", auth)
def convert_post():
    user = auth.get_user()
    f = request.files["File"]
    filename = "apps/neww/uploads/{0}-{1}".format(random.randint(0, 10000), f.filename)
    f.save(filename)
    result, error_kod = excel_todb.main(2, [filename, 1], user['id'])
    error = db(db.errors.kod == error_kod).select()[0].ua
    if result[0] == 'OK':
        f1, f2, f3 = server_func.df_tonfp_iar(result[1], result[3], result[4])
    else:
        f1, f2, f3 = '', '', ''
    return dict(user=user, f1=f1, f2=f2, f3=f3, message=result[0], error=error)

@action("convert_ir2",method="GET")
@action.uses("convert_ir2.html", auth)
def convert_get():
    user = auth.get_user()
    return dict(user=user, session=session)

@action("static/convert_ir2",method="POST")
@action.uses("convert3.html", auth)
def convert_post():
    user = auth.get_user()
    ff = request.files.getall('File')
    files = []
    for f in ff:
        filename = "apps/neww/uploads/{0}-{1}".format(random.randint(0, 10000), f.filename)
        files.append(filename)
        f.save(filename)
    file = server_func.ir2_convert(files)
    return dict(user=user, f=file)


@action("upload",method="GET")
@action.uses("upload_file.html", auth)
def upload_get():
    c = []
    b = []
    t = []
    user = auth.get_user()
    if len(user) > 0:
        for i in db(db.company_user).select():
            if i.site_user == user['id'] or user['id'] == 1:
                j = db(db.company.id == i.company_id).select()[0]
                c.append(j.IAN_FULL_NAME)
                b.append(j.id)
        for i in db(db.nbu_tables).select():
            t.append(i.name)
    return dict(message="",new_data_dict={},session=session,companies=c, comp=b, user=user, tables=t)


@action("static/upload",method="POST")
@action.uses("upload2.html", db, auth)
def upload_post():
    user = auth.get_user()
    table=int(request.POST['table'])
    c=request.POST['company']
    users_block[user['id']] = [table, c]
    return dict(table=table, user=user)

@action("static/upload2",method="POST")
@action.uses("confirm.html", db, auth)
def upload2_post():
    log111 = open('log111.txt', 'w')
    user = auth.get_user()
    [table, c] = users_block[user['id']]
    if table < 3:
        quarter=int(request.POST['quarter'])
        year=int(request.POST['year'])
    if table != 5:
        k=int(request.POST['k'])
    f = request.files["File"]
    filename = "apps/neww/uploads/{0}-{1}".format(random.randint(0, 10000), f.filename)
    f.save(filename)
    if table < 3:
        log = open('history.log', 'a')
        result, error_kod = excel_todb.main(4, [filename, table, k], user['id'])
        log.write("User_id: {0};\tuploaded file: {1};\tcompany_id: {2},\tquarter: {3};\tyear: {4};\n".format(
            user['id'], filename[18:], c, quarter, year))
        a = [result[1], int(quarter), int(year), db(db.company.id == c).select()[0].id, table]
        users_block[user['id']] = a
        log.close()
    elif table == 3 or table == 4:
        log = open('history.log', 'a')
        result, error_kod = excel_todb.main(4, [filename, table, k], user['id'])
        log.write("User_id: {0};\tuploaded file: {1};\tcompany_id: {2},\n".format(
            user['id'], filename[18:], c))
        a = [result[1], db(db.company.id == c).select()[0].id, table]
        users_block[user['id']] = a
        log.close()
    elif table == 5:
        log = open('history.log', 'a')
        result, error_kod = excel_todb.main(6, filename, user['id'])
        log.write("User_id: {0};\tuploaded file: {1};\tcompany_id: {2}\n".format(
            user['id'], filename, c))
        a = [result, db(db.company.id == c).select()[0].id, table]
        users_block[user['id']] = a
        log.close()
    elif table == 6:
        log = open('history.log', 'a')
        result, error_kod = excel_todb.main(2, [filename, k], user['id'])
        log111.write(str(result))
        log.write("User_id: {0};\tuploaded file: {1};\tcompany_id: {2},\tquarter: {3};\tyear: {4};\n".format(
            user['id'], filename, c, result[3], result[4]))
        a = [result[1], result[3], result[4], db(db.company.id == c).select()[0].id, table]
        users_block[user['id']] = a
        log.close()
    else:
        log = open('history.log', 'a')
        result, error_kod = excel_todb.main(5, [filename, table-6, k], user['id'])
        log111.write(str(result))
        log.write("User_id: {0};\tuploaded file: {1};\tcompany_id: {2},\tquarter: {3};\tyear: {4};\n".format(
            user['id'], filename, c, result[3], result[4]))
        a = [result[1], result[3], result[4], db(db.company.id == c).select()[0].id, table]
        users_block[user['id']] = a
        log.close()
    log111.write(str(result))
    log111.close()
    error = db(db.errors.kod == error_kod).select()[0].ua
    return dict(user=user, result_all=result, table=table, error=error)

@action("static/confirm")
@action.uses("result_page.html", auth)
def index():
    user = auth.get_user()
    text, error_kod = excel_todb.main(3, users_block[user['id']], user['id'])
    error = db(db.errors.kod == error_kod).select()[0].ua
    return dict(message=text, user=user, url='', error=error, df=[])

@action("add_company",method="GET")
@action.uses("add_company_page.html", auth)
def add_company_get():
    user = auth.get_user()
    return dict(user=user)


@action("static/add_company",method="POST")
@action.uses("result_page.html", db, auth)
def add_company_post():
    user = auth.get_user()
    m = {}
    fields = db.company.fields
    m[fields[1]]=request.POST['name']
    m[fields[2]]=request.POST['type']
    m[fields[3]]=request.POST['kod']
    m[fields[4]]=request.POST['series']
    m[fields[5]]=request.POST['number']
    m[fields[6]]=request.POST['date']
    m[fields[7]]=request.POST['status']
    m[fields[8]]=request.POST['adres']
    m[fields[9]]=request.POST['tel_kod']
    m[fields[10]]=request.POST['tel']
    m[fields[11]]=request.POST['email']
    m[fields[12]]=request.POST['obl']
    m[fields[13]]=request.POST['pib']
    m[fields[15]]=request.POST['position']
    m[fields[14]]=request.POST['abbreviation']
    m[fields[16]]=datetime.datetime.today()
    db.company.insert(**m)
    db.company_user.insert(site_user=1, company_id=db(db.company).select()[-1].id)
    message = "Інформація про компанію успішно додана"
    return dict(message=message, user=user, url='')

@action("add_table",method="GET")
@action.uses("add_table.html", auth)
def add_table_get():
    user = auth.get_user()
    return dict(user=user, session=session)

@action("static/add_table",method="POST")
@action.uses("result_page.html", auth)
def add_table_post():
    log333 = open('log333.txt', 'w')
    user = auth.get_user()
    name=request.POST['name']
    schema=request.POST['schema']
    p_schema = "apps/neww/static/{}.xsd".format(name)
    try:
        schema.save(p_schema)
    except OSError:
        pass
    ff = request.files["File"]
    keyss = request.files.getall('Keys')
    reg = "apps/neww/static/csv/{}".format(ff.filename)
    try:
        ff.save(reg)
    except OSError:
        pass
    keys = []
    for i in range(len(keyss)):
        keys.append("apps/neww/static/csv/{0}{1}".format(random.randint(100, 999), keyss[i].filename))
        try:
            keyss[i].save(keys[i])
        except OSError:
            pass
    f = xlrd.open_workbook(reg)
    page = f.sheet_by_index(1)
    N_T = 0
    N_ID = 0
    for i in range(6):
        for j in range(page.ncols):
            if page.cell_value(i, j) == 'Метрика':
                N_T = j
            if "ID" in str(page.cell_value(i, j)):
                N_ID = j
    if N_T == page.ncols:
        param = []
        names = ''
    else:
        param = {}
        for i in keyss:
            s = str(i.filename)
            param[s[:s.index('.')]] = 0
        names = [i for i in param]
        log333.write(str(param)+'\n'+str(names)+'\n')
        K = {}
        for i in range(len(keys)):
            f_k = xlrd.open_workbook(keys[i])
            page_k = f_k.sheet_by_index(0)
            c = []
            for j in range(page_k.nrows):
                if j == 0: continue
                c.append(str(page_k.cell_value(j, 0))[0])
            K[names[i]] = c
        for i in range(6):
            for j in range(page.ncols):
                for z in param:
                    if page.cell_value(i, j) == z:
                        param[z] = j
    T_list = []
    for j in range(6):
        if page.cell_value(j, N_T) == '':
            pass
        elif str(page.cell_value(j, N_T))[0] == 'T' or str(page.cell_value(j, N_T))[0] == 'Т':
            if N_T == page.ncols:
                T_list.append('t'+str(page.cell_value(j, N_T))[1:])
                if T_list == []:
                    T_list = ['t100']
            else:
                for i in range(min(param.values()) - N_T):
                    T_list.append('t'+str(page.cell_value(j, N_T+i))[1:])
            break
    if T_list == []:
        for i in range(min(param.values()) - N_T):
            log333.write('---\n')
            T_list.append('t100_'+str(i+1))
    db.define_table(name,
                Field('ekp', type='string'),
                [Field(i, type='integer') for i in T_list],
                [Field(i, type='string') for i in param],
                Field('quarter', type='integer'),
                Field('year', type='integer'),
                Field('company_id', 'reference company'),
                Field('upload_date', type='datetime', default = datetime.date.today()))
    if N_T != page.ncols:
        db.define_table(name+'_description',
                    Field('ekp', type='string'),
                    [Field(i, type='string') for i in param])
    db.commit()
    text = "\ndb.define_table('"+str(name)+"', Field('ekp', type='string'),"
    for i in T_list:
        text += " Field('"+str(i)+"', type='integer'),"
    for i in param:
        text += " Field('"+str(i)+"', type='string'),"
    text += " Field('quarter', type='integer'), Field('year', type='integer'), Field('company_id', 'reference company'), Field('upload_date', type='datetime', default = datetime.date.today()))"
    if len(param) > 0:
        text += "\ndb.define_table('"+str(name)+"_description', Field('ekp', type='string')"
        for i in param:
            text += ", Field('"+str(i)+"', type='string')"
        text += ")"
    f = open('apps/neww/models.py', 'r')
    string = f.read()
    d = string[-12:]
    f.close()
    f = open('apps/neww/models.py', 'w')
    f.write(string[:-12])
    f.write(text)
    f.write(d)
    f.close()
    for i in range(page.nrows):
        if N_T == page.ncols:
            break
        if name in str(page.cell_value(i, N_ID)):
            m = {}
            ekp = str(page.cell_value(i, N_ID))
            for i1 in param:
                if '≠ #' in page.cell_value(i, param[i1]) or '≠#' in page.cell_value(i, param[i1]):
                    m[i1] = K[i1][:-1]
                elif '#' in page.cell_value(i, param[i1]):
                    m[i1] = ['#']
                elif '≠' in page.cell_value(i, param[i1]):
                    c = page.cell_value(i, param[i1])[page.cell_value(i, param[i1]).index('≠'):]
                    m[i1] = K[i1]
                    for j in c:
                        try:
                            m[i1].remove(j)
                        except ValueError:
                            pass
                else:
                    m[i1] = []
                    c = page.cell_value(i, param[i1])[page.cell_value(i, param[i1]).index('='):]
                    for j in c:
                        if j in K[i1]:
                            m[i1].append(j)
            if len(m) == 1:
                for i1 in m[names[0]]:
                    c = {'ekp':ekp, names[0]: i1}
                    db[str(name)+'_description'].insert(**c)
            elif len(m) == 2:
                for i1 in m[names[0]]:
                    for i2 in m[names[1]]:
                        c = {'ekp':ekp, names[0]: i1, names[1]: i2}
                        db[str(name)+'_description'].insert(**c)
            elif len(m) == 3:
                for i1 in m[names[0]]:
                    for i2 in m[names[1]]:
                        for i3 in m[names[2]]:
                            c = {'ekp':ekp, names[0]: i1, names[1]: i2, names[2]: i3}
                            db[str(name)+'_description'].insert(**c)
            elif len(m) == 4:
                for i1 in m[names[0]]:
                    for i2 in m[names[1]]:
                        for i3 in m[names[2]]:
                            for i4 in m[names[3]]:
                                c = {'ekp':ekp, names[0]: i1, names[1]: i2, names[2]: i3, names[3]: i4}
                                db[str(name)+'_description'].insert(**c)
            elif len(m) == 5:
                for i1 in m[names[0]]:
                    for i2 in m[names[1]]:
                        for i3 in m[names[2]]:
                            for i4 in m[names[3]]:
                                for i5 in m[names[4]]:
                                    c = {'ekp':ekp, names[0]: i1, names[1]: i2, names[2]: i3, names[3]: i4, names[4]: i5}
                                    db[str(name)+'_description'].insert(**c)
    log333.close()
    return dict(message="Операция успешно завершена", user=user, url='', error="OK", df=[])

@action("info_db",method="GET")
@action.uses("admin_page.html", auth)
def info_db_get():
    message = ""
    user = auth.get_user()
    return dict(session=session, user=user, message=message)