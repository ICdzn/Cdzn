from py4web import action, request, Session
from .models import db
from .common import *
from .classes import Updates,DatabaseAccess,DataModify
from py4web.utils.form import Form, FormStyleBulma

import smtplib
import pandas as pd
import os
import datetime


@authenticated()
def index():
    user=my_auth.get_user()
    print(user)
    rows = db(db.company).select()
    return dict(rows=rows)

@authenticated()
def license():
    try:
        rows = db(db.company).select()
    except: rows=[]
    return dict(rows=rows)

@action("access")
@action.uses(db)
def access():
    obj=DatabaseAccess()
    codes=obj.get_codes('0692')
    name = obj.get_full_name('20693867')
    adr = obj.get_address('20693867')
    di = obj.get_director('20693867')
    print(codes)
    print(name)
    print(adr)
    print(di)
    return 'OK'

@authenticated()
def add_company():
    # db(db.company.id>0).delete()
    # db(db.license.id>0).delete()
    user_id = my_auth.get_user()['id']
    user = db(db.site_user.auth_user == user_id).select().first()
    if not user:
        db['site_user'].insert(auth_user=user_id)
    last_company = db(db.company).select().last()
    last_data = last_company.update_date
    rows = db(db.company.update_date == last_data).select()
    auth_user = db(db.auth_user.id==user_id).select().first()
    site_user = db(db.site_user.auth_user == user_id).select().first()
    form1 = Form(db.auth_user, auth_user, deletable=False, formstyle=FormStyleBulma)
    print(form1)
    form2 = Form(db.site_user, site_user, deletable=False, formstyle=FormStyleBulma)
    return dict(rows=rows,auth_user=auth_user,site_user=site_user,form1=form1, form2=form2)

@action("update_database",method="GET")
@action.uses(db)
def update_database():
    db_obj = DatabaseAccess()
    obj = Updates()
    modify_obj = DataModify()
    try:
        data = db_obj.get_update_data()
        now = datetime.datetime.now()
        if (now - data).days > 7:
            tuple_obj = obj.parser()
            obj.compare(tuple_obj[2], tuple_obj[0])
            modify_obj.modify_company(tuple_obj[0])
            modify_obj.modify_license(tuple_obj[1])
            print(db_obj.upload_companies(tuple_obj[0])+'1')
            print(db_obj.upload_licenses(tuple_obj[1])+'1')
    except AttributeError:
        tuple_obj=obj.parser()
        obj.compare(tuple_obj[2],tuple_obj[0])
        modify_obj.modify_company(tuple_obj[0])
        modify_obj.modify_license(tuple_obj[1])
        print(db_obj.upload_companies(tuple_obj[0])+'2')
        print(db_obj.upload_licenses(tuple_obj[1])+"2")
    return 'Hello'

@action("static/add_company",method="POST")
@action.uses("add_company.html", db)
def add():
    db_obj = DatabaseAccess()
    last_company = db(db.company).select().last()
    last_data = last_company.update_date
    rows = db(db.company.update_date == last_data).select()
    choice_id = request.POST.get('choice_id')
    action = request.POST.get('action')
    current_company = db(db.company.id == choice_id).select().last()
    user_id = my_auth.get_user()['id']
    site_user = db_obj.get_user(user_id)
    auth_user = db_obj.get_auth_user(user_id)
    fromaddr = 'strahovka.work2020@gmail.com'
    toaddr = auth_user.email
    # toaddr = current_company.email

    username = 'strahovka.work2020@gmail.com'
    password = 'cdnblpUYBvdlH8'
    server = smtplib.SMTP('smtp.gmail.com:587')
    if action == 'add':
        db_obj.insert_request(site_user, choice_id, action)
        server.starttls()
        server.login(username, password)
        msg1 = 'Потвердите что вашу компанию обслуживает ' + str(site_user.first_name)
        msg2 = 'Confirm that you want to add this company' + str(current_company.IAN_FULL_NAME) + str(
            current_company.IM_NUMIDENT)
        server.sendmail(fromaddr, toaddr, msg2.encode("utf8"))
        server.quit()
    elif action == 'delete':
        db_obj.insert_request(site_user, choice_id, action)
        server.starttls()
        msg3 = 'Confirm that you want to delete this company' + str(current_company.IAN_FULL_NAME) + str(
            current_company.IM_NUMIDENT)
        server.login(username, password)
        server.sendmail(fromaddr, toaddr, msg3.encode("utf8"))
        server.quit()
    form1 = Form(db.auth_user, auth_user, deletable=False, formstyle=FormStyleBulma)
    form2 = Form(db.site_user, site_user, deletable=False, formstyle=FormStyleBulma)
    return dict(rows=rows,auth_user=auth_user,site_user=site_user,session=session,form1=form1, form2=form2)

@action("update_company_user")
def add_request():
    db_obj = DatabaseAccess()
    db_obj.update_company_user()

@action("company_users",method="GET")
@action.uses("company_users.html", db)
def add():
    rows = db(db.request).select()
    return dict(rows=rows)

@authenticated()
def upload():
    return dict(message="",new_data_dict={},session=session)

@action("static/upload",method="POST")
@action.uses("confirm.html", db)
def upload_post():

    f = request.files["neededFile"]
    table_num = request.POST['table']
    quarter=request.POST['quarter']
    table = tables[table_num]

    f.save('apps/strahovka/xls_files/'+f'{f.filename}')
    message='Файл успешно загружен'
    #pandas and excel
    if table=='payout':
        data_xls=pd.read_excel('apps/strahovka/xls_files/'+f'{f.filename}', index_col=None, skiprows=1, dtype=str)
        new_data_dict = data_xls.to_dict(orient='records')
        print(new_data_dict)
    elif table=='type':
        data_xls = pd.read_excel('apps/strahovka/xls_files/' + f'{f.filename}', index_col=1, dtype=str)
        data_xls = data_xls.T
        print(data_xls)
        data_dict = data_xls.to_dict(orient='index')
        print(data_dict)

        new_data_dict = []
        for key,values_dict in data_dict.items():
            if key=='Назва' or key=='Всього:':
                continue
            new_dict = {}
            new_dict['Назва']=key
            for key2,value2 in values_dict.items():
                for key1, value1 in names[table].items():
                    if key2 == value1:
                        new_dict[key2]=value2
            new_data_dict.append(new_dict)
        print(new_data_dict)
    else:
        data_xls = pd.read_excel('apps/strahovka/xls_files/' + f'{f.filename}')
    rows=db(db.company_user.user == globals().get('session').get('user').get('id')).select()
    message = 'Error'
    for row in rows:
        if str(row.company_id)==new_data_dict[0]['Номер компанії']:
            message = 'Thank you'
            break
    if message != 'Error':
        data = {'message':'Файл успешно загружен', 'new_data_dict':new_data_dict, 'table':table,
        'filename':f.filename, 'quarter':quarter, 'session':session}
    else:
        data = {'message': 'Error','session':session}
    return data

@action("static/confirm",method="POST")
@action.uses("confirm_success.html", db)
def confirm_post():
    table=request.POST['tabl_name']
    file_name=request.POST['file_name']
    quarter = request.POST['quarter']
    print(file_name)
    if table=='payout':
        data_xls=pd.read_excel('apps/strahovka/xls_files/'+f'{file_name}', index_col=None, skiprows=1, dtype=str)
        data_dict = data_xls.to_dict(orient='records')
        new_data_dict=[]
        for i in range(len(data_dict)):
            new_dict = {}
            for key2, value2 in data_dict[i].items():
                for key1,value1 in names['payout'].items():
                    if key2 == value1 or key2 in value1:
                        new_dict[key1]=value2
            new_data_dict.append(new_dict)
        for i in range(len(new_data_dict)):
            db['payout'].insert(**new_data_dict[i])
        print(new_data_dict)
    elif table=='type':
        data_xls = pd.read_excel('apps/strahovka/xls_files/' + f'{file_name}', index_col=1, dtype=str)
        data_xls = data_xls.T
        print(data_xls)
        data_dict = data_xls.to_dict(orient='index')
        print(data_dict)

        new_data_dict = []
        for key,values_dict in data_dict.items():
            if key=='Назва' or key=='Всього:':
                continue
            new_dict = {}
            new_dict['name']=key
            for key2,value2 in values_dict.items():
                for key1, value1 in names['type'].items():
                    if key2 == value1:
                        new_dict[key1]=value2
            new_data_dict.append(new_dict)
        for i in range(len(new_data_dict)):
            db['type'].insert(**new_data_dict[i])
        print(new_data_dict)
    today_data=str(datetime.datetime.now())
    company_id=new_data_dict[0]['company_id']
    user_name=globals().get('session').get('user').get('id')
    db['download'].insert(name=table, data=today_data,quarter_num=quarter,user=user_name,company_id=company_id)
    os.remove('apps/strahovka/xls_files/' + f'{file_name}')
    data={'message':'Thank you','session':session}

    fromaddr = 'testvlada222@gmail.com'
    toaddr = db(db.auth_user.id==user_name).select().first().email
    msg = 'Hello'
    # Gmail Login

    username = 'testvlada222@gmail.com'
    password = 'pydal4weBB'

    # Sending the mail

    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username, password)
    server.sendmail(fromaddr, toaddr, msg)
    server.quit()
    return data

@action("auth")
@action.uses("auth.html")
def auth():
    message='Hello'
    return dict(message=message)
