from . import db
import xlrd
import pandas as pd
import datetime
#def new_company():
#    m = {}
#    for i in db.company.fields:
#        if i == 'id': continue
#        m.update({i: 'djhjhhjvjvggcf'})
#    db.company.insert(**m)
#if len(db(db.company).select()) == 0:
#    new_company()
def db34(df, quarter, year):
    f = open('textt.txt', 'w')
    for i1 in range(df.shape[0]):
        for i2 in range(df.shape[1]):
            if i2 == 0:
                m = {db.type.fields[1]: df[i2][i1]}
            elif i2 == df.shape[1]-1:
                m.update({db.type.fields[i2+1]: int(df[i2][i1])})
            else:
                a = str(df[i2][i1])
                for i in range(len(a)):
                    try:
                        if a[i] == ' ':
                            a = a[:i] + a[i+1:]
                        if a[i] == ',':
                            a = a[:i] + '.' + a[i + 1:]
                    except IndexError:
                        pass
                a = float(a)
                m.update({db.type.fields[i2+1]: a})
        c = True
        for i in db(db.type.name == m['name']).select():
            if i.quarter == quarter and i.year == year and i.company_id == m['company_id']:
                i.update_record(**m)
                c = False
                break
        if c is True:
            db.type.insert(**m)
            f.write(str(db(db.type).select()[-1]) + '\n')
    f.close()

def payout_todb(df):
    f = open('textt.txt', 'w')
    for i in df.columns:
        if df[i].notnull().sum() == 0:
            del df[i]
    for i in range(df.shape[0]):
        if df.loc[i].isnull().sum() > 0:
            df.drop([i], inplace=True)
    flip = False
    for i1 in range(df.shape[0]):
        for i2 in range(5):
            a = str(df[i2+3][i1])
            for i in range(len(a)):
                if a[i] in ' ., /_':
                    a = a[:i] + '-' + a[i + 1:]
            if len(a) == 10 and len(a[:a.index('-')]) == 2:
                a = a[6:] + '-' + a[3:6] + a[:2]
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
            if flip is False:
                if int(a[5:7]) > 12:
                    flip = True
            df[i2 + 3][i1] = a
    if flip is True:
        for i1 in range(df.shape[0]):
            for i2 in range(5):
                a = df[i2 + 3][i1]
                df[i2 + 3][i1] = a[:4] + a[7:] + a[4:7]
    for i1 in range(df.shape[0]):
        m = {}
        for i2 in df.columns:
            if i2 == 0:
                m.update({db.payout.fields[i2 + 1]: df[i2][i1]})
            elif i2 == 1 or i2 == 2:
                m.update({db.payout.fields[i2 + 1]: int(df[i2][i1])})
            elif i2 > 7 and i2 < 11:
                a = str(df[i2][i1])
                for i in range(len(a)):
                    try:
                        if a[i] == ' ':
                            a = a[:i] + a[i + 1:]
                        if a[i] == ',':
                            a = a[:i] + '.' + a[i + 1:]
                    except IndexError:
                        pass
                a = float(a)
                m.update({db.payout.fields[i2 + 1]: a})
            elif i2 > max(df.columns)-3:
                m.update({db.payout.fields[i2-max(df.columns)-1]: int(df[i2][i1])})
            else:
                a = df[i2][i1]
                a = datetime.date(int(a[:4]), int(a[5:7]), int(a[8:]))
                m.update({db.payout.fields[i2 + 1]: a})
        db.payout.insert(**m)
        f.write(str(db(db.payout).select()[-1]) + '\n')
    f.close()

def import_excel(xl, quarter, year, c_id):
    f = xlrd.open_workbook(xl)
    for sheet in range(f.nsheets):
        page = f.sheet_by_index(sheet)
        error = 0
        if page.ncols > 9 and page.ncols < 19:
            t = []
            c = True
            for i in range(6):
                if c is False: break
                for j in range(4):
                    if c is False: break
                    if page.cell_value(i, j) == '': continue
                    try:
                        if int(page.cell_value(i, j)) > 0 and int(page.cell_value(i, j+1)) > 0 and j != 0:
                            for i1 in range(page.nrows - i):
                                t.append([page.cell_value(i+i1, j-1)])
                                for i2 in range(page.ncols - j):
                                    t[i1].append(page.cell_value(i+i1, j+i2))
                                t[i1].append(quarter)
                                t[i1].append(year)
                                t[i1].append(c_id)
                            df = pd.DataFrame(t)
                            c = False
                    except ValueError:
                            pass
            try:
                payout_todb(df)
            except ValueError:
                l = open('apps/neww/uploads/textt14.txt', 'w')
                l.write("-")
                l.close()
        else:
            if page.ncols > 22 and page.ncols < 28:
                a = 0
            elif page.ncols > 41 and page.ncols < 47:
                a = 22
            else:
                a = -1
            if a == -1:
                error = 1
                break
            else:
                t = []
                c = True
                for i in range(15):
                    if c is False: break
                    for j in range(3):
                        if c is False: break
                        if page.cell_value(i, j) == '': continue
                        try:
                            if int(page.cell_value(i, j)) == 10 and int(page.cell_value(i+1, j)) == 11 and int(
                                    page.cell_value(i+2, j)) == 12:
                                y = i
                                for i in range(page.ncols-j-1):
                                    if page.cell_value(y, j+i+1) == '': continue
                                    try:
                                        if int(page.cell_value(y, j+i+1)) >= 0:
                                            x = j+i+2
                                            break
                                    except ValueError:
                                        pass
                                for i1 in range(page.ncols - x):
                                    t.append([])
                                    for i in range(y):
                                        try:
                                            if int(page.cell_value(y-i, x+i1)) >= 0:
                                                pass
                                        except ValueError:
                                            t[i1].append(page.cell_value(y-i, x+i1))
                                            break
                                    for i2 in range(page.nrows - y):
                                        if page.cell_value(y+i2, j) == '': continue
                                        try:
                                            if int(page.cell_value(y+i2, j)) >= 0:
                                                t[i1].append(page.cell_value(y+i2, x+i1))
                                        except ValueError:
                                            pass
                                    t[i1].append(quarter)
                                    t[i1].append(year)
                                    t[i1].append(c_id)
                                df = pd.DataFrame(t)
                                c = False
                                break
                        except ValueError:
                            pass
                if c is True:
                    for i in range(15):
                        if c is False: break
                        for j in range(6):
                            if c is False: break
                            if page.cell_value(i, j) == '': continue
                            try:
                                if float(page.cell_value(i, j)) != float(page.cell_value(i, j+1)) - 1 and float(
                                        page.cell_value(i, j)) != float(page.cell_value(i, j+2)) - 2:
                                    x = j+1
                                    y = i
                                    for i1 in range(page.ncols - x):
                                        t.append([])
                                        if len(page.cell_value(y-1, x+i1)) <= 3:
                                            t[i1].append(page.cell_value(y-1, x+i1))
                                        else:
                                            t[i1].append(page.cell_value(y-2, x+i1))
                                        for i2 in range(page.nrows - y):
                                            t[i1].append(page.cell_value(y+i2, x+i1))
                                        t[i1].append(quarter)
                                        t[i1].append(year)
                                        t[i1].append(c_id)
                                    df = pd.DataFrame(t)
                                    c = False
                                    break
                            except ValueError:
                                pass
                try:
                    db34(df, quarter, year)
                    l = open('apps/neww/uploads/textt15.txt', 'w')
                    l.write("-")
                    l.close()
                except NameError:
                    l = open('apps/neww/uploads/textt13.txt', 'w')
                    l.write("-")
                    l.close()

