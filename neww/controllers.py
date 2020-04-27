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
from . import task01
import datetime, random

@authenticated()
def home():
    user = auth.get_user()
    message = T("Hello {first_name}".format(**user))
    return dict(message=message, user=user)

def func(form):
    if not form.errors:
        id = 0
        for i in db(db.company).select():
            if form.vars['company'] == i.IAN_FULL_NAME:
                f = open('apps/neww/uploads/textt.txt', 'w')
                f.write(str(form.vars['file']))
                f.close()
                db.person.insert(name=form.vars['company'], file=form.vars['file'])
                break

@action('stranichka025', method=['GET', 'POST'])
@action.uses('stranichka025.html', session)
def insertt():
    c = []
    for i in db(db.company).select():
        c.append(i.IAN_FULL_NAME)
    form = Form([
        Field('company', requires=IS_IN_SET(c)),
        Field('file', 'upload')],
        validation=func,
        formstyle=FormStyleBulma)
    return dict(form=form)

@action("upload",method="GET")
@action.uses("upload_file.html")
def upload_get():
    c = []
    for i in db(db.company).select():
        c.append(i.IAN_FULL_NAME)
    return dict(message="",new_data_dict={},session=session,companies=c)


@action("static/upload",method="POST")
@action.uses("confirm.html", db)
def upload_post():
    f = request.files["File"]
    quarter=request.POST['quarter']
    year=request.POST['year']
    c=request.POST['company']
    filename = "apps/neww/uploads/{0}-{1}".format(random.randint(0, 10000), f.filename)
    f.save(filename)
    for i in db(db.company).select():
        if i.IAN_FULL_NAME == c:
            task01.import_excel(filename, int(quarter), int(year), i.id)
    return dict(message="OK")

@action("show_db")
def show():
    m = []
    m1 = []
    for i in db(db.payout).select():
        m.append(str(i))
    for i in db(db.type).select():
        m1.append(str(i))
    return dict(m1=m,m2=m1)