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
users_block = {}
for i in db(db.company_user).select():
    users_block[i.site_user] = 0



@authenticated()
@action.uses(auth)
def index():
    user = auth.get_user()
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
def convert_post():
    user = auth.get_user()
    c = request.POST['company']
    ff = request.files.getall('File')
    f_names = []
    result_all = []
    for f in ff:
        filename = "apps/neww/uploads/{0}-{1}".format(random.randint(0, 10000), f.filename)
        f.save(filename)
        f_names.append(filename)
    for file in f_names:
        text = 'Операция успешно завершена'
        if '.xml' in file:
            result, error_kod = excel_todb.main(2, [file, 1], user['id'])
            text, error_kod = excel_todb.main(3, [result[1], result[3], result[4], db(db.company.id == c).select()[0].id, 5], user['id'])
        elif 'ІАР' in file:
            result, error_kod = excel_todb.main(5, file, user['id'])
            text, error_kod = excel_todb.main(3, [result, db(db.company.id == c).select()[0].id, 6], user['id'])
        elif 'Розділ-3' in file:
            quarter, year = int(file[0]), int(file[2:6])
            result, error_kod = excel_todb.main(4, [file, 1, 100000], user['id'])
            text, error_kod = excel_todb.main(3, [result[1], int(quarter), int(year), db(db.company.id == c).select()[0].id, 1], user['id'])
        elif 'Розділ-4' in file:
            quarter, year = int(file[0]), int(file[2:6])
            result, error_kod = excel_todb.main(4, [file, 2, 100000], user['id'])
            text, error_kod = excel_todb.main(3, [result[1], int(quarter), int(year), db(db.company.id == c).select()[0].id, 2], user['id'])
        elif 'Журнал' in file:
            result, error_kod = excel_todb.main(4, [file, 3, 100], user['id'])
            text, error_kod = excel_todb.main(3, [result[1], db(db.company.id == c).select()[0].id, 3], user['id'])
        elif 'Резерв' in file:
            result, error_kod = excel_todb.main(4, [file, 4, 100], user['id'])
            text, error_kod = excel_todb.main(3, [result[1], db(db.company.id == c).select()[0].id, 4], user['id'])
        else:
            pass
        if text != 'Операция успешно завершена':
            error = db(db.errors.kod == error_kod).select()[0].ua + '  /// Ошибка произошла в файле {}'.format(file)
            break
        else:
            error = 'OK'
    return dict(message=text, user=user, url='', error=error)

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