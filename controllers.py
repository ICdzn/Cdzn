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
import datetime, random
import pandas as pd
from lxml import etree, html
users_block = {}
for i in db(db.company_user).select():
    users_block[i.site_user] = 0

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
    df = pd.DataFrame(columns = ['type', 'payout', 'rezerv', 'quarter', 'year'])
    for i in range((y_max-y_min)*4+q_max-q_min+1):
        quarter = (q_min + i) % 4
        if quarter == 0:
            quarter = 4
            year = y_min + int((q_min + i) / 4) - 1
        else:
            year = y_min + int((q_min + i) / 4)
        df.loc[df.shape[0]] = [[True if len(db((db.type.company_id == c) & (db.type.year == year) & (db.type.quarter == quarter)).select()) > 0 else False]
        , False, False, quarter, year]
    log444.write(str(df))
    log444.close()
    return dict(message=text, user=user, url='', error=error, table=df)

@action("convert",method="GET")
@action.uses("convert.html", auth)
def convert_get():
    user = auth.get_user()
    return dict(user=user, session=session)

@action("static/convert",method="POST")
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


@action("upload",method="GET")
@action.uses("upload_file.html", auth)
def upload_get():
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
    if table != 6:
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
    elif table == 5:
        log = open('history.log', 'a')
        result, error_kod = excel_todb.main(2, [filename, k], user['id'])
        log111.write(str(result))
        log.write("User_id: {0};\tuploaded file: {1};\tcompany_id: {2},\tquarter: {3};\tyear: {4};\n".format(
            user['id'], filename, c, result[3], result[4]))
        a = [result[1], result[3], result[4], db(db.company.id == c).select()[0].id, table]
        users_block[user['id']] = a
        log.close()
    elif table == 6:
        log = open('history.log', 'a')
        result, error_kod = excel_todb.main(5, filename, user['id'])
        log.write("User_id: {0};\tuploaded file: {1};\tcompany_id: {2}\n".format(
            user['id'], filename, c))
        a = [result, db(db.company.id == c).select()[0].id, table]
        users_block[user['id']] = a
        log.close()
    else:
        log = open('history.log', 'a')
        result, error_kod = excel_todb.main(4, [filename, table, k], user['id'])
        log.write("User_id: {0};\tuploaded file: {1};\tcompany_id: {2},\n".format(
            user['id'], filename[18:], c))
        a = [result[1], db(db.company.id == c).select()[0].id, table]
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
    return dict(message=text, user=user, url='', error=error)

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

@action("info_db",method="GET")
@action.uses("admin_page.html", auth)
def info_db_get():
    message = ""
    user = auth.get_user()
    return dict(session=session, user=user, message=message)