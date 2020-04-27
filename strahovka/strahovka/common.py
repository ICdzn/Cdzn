import os
import sys
import logging
from py4web import Session, Cache, Translator, DAL, Field
from py4web.utils.mailer import Mailer
from py4web.utils.auth import Auth
from py4web.utils.tags import Tags
from py4web.utils.factories import ActionFactory
from . import settings

names = {'company': {'IAN_FULL_NAME': 'Найменування',
       'FIN_TYPE': 'Тип ФУ',
       'IM_NUMIDENT': 'Код ЄДРПОУ',
       'IAN_RO_SERIA': 'Серія свідоцтва про реєстрацію',
       'IAN_RO_CODE': 'Номер свідоцтва про реєстрацію',
       'IAN_RO_DT': 'Дата реєстрації',
       'DIC_NAME': 'Статус',
       'F_ADR': 'Адреса',
       'IA_PHONE_CODE': 'Міжміський телефонний код',
       'IA_PHONE': 'Телефон',
       'IA_EMAIL': 'E-mail',
       'IND_OBL': 'Область',
       'K_NAME': 'ПІБ керівника',
       'IM_IH': 'Детально'},
       'payout': {'insurance_type': 'Вид страхування',
       'contract_num': '№ договору страхування',
       'case_num': 'Обліковий № справи про вимогу',
       'insurance_case_date': 'Дата настання страхового випадку',
       'statement_date': 'Дата заяви страхового випадку',
       'requirement_date': 'Дата вимоги на виплату',
       'insurance_act_date': 'Дата страхового акту',
       'insurance_payment_date': 'Дата страхової виплати',
       'insurance_payment_size': 'Розмір страхової виплати, грн.',
       'settlement_costs': 'Витрати на врегулювання, грн.',
       'reserve_size': 'Розмір сформованого резерву на звітну дату',
       'company_id': 'Номер компанії'},
       'type': {'gross_receipts': 10,
       'from_residents': 11,
       'rfrom_insurers_individuals': 12,
       'rfrom_insurers_legal_entities': 13,
       'rfrom_reinsurers': 14,
       'from_non_residents': 15,
       'nfrom_insurers_individuals': 16,
        'company_id':'Номер компанії'}}


tables={'1': 'company','2': 'type','3': 'payout'}

# implement custom loggers form settings.LOGGERS
logger = logging.getLogger("py4web:" + settings.APP_NAME)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
)
for item in settings.LOGGERS:
    level, filename = item.split(":", 1)
    if filename in ("stdout", "stderr"):
        handler = logging.StreamHandler(getattr(sys, filename))
    else:
        handler = logging.FileHandler(filename)
    handler.setLevel(getattr(logging, level.upper(), "ERROR"))
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# connect to db
db = DAL(settings.DB_URI, folder=settings.DB_FOLDER, pool_size=settings.DB_POOL_SIZE,
         migrate_enabled=True)

# define global objects that may or may not be used by th actions
cache = Cache(size=1000)
T = Translator(settings.T_FOLDER)

# pick the session type that suits you best
if settings.SESSION_TYPE == "cookies":
    session = Session(secret=settings.SESSION_SECRET_KEY)
elif settings.SESSION_TYPE == "redis":
    import redis

    host, port = settings.REDIS_SERVER.split(":")
    # for more options: https://github.com/andymccurdy/redis-py/blob/master/redis/client.py
    conn = redis.Redis(host=host, port=int(port))
    conn.set = lambda k, v, e, cs=conn.set, ct=conn.ttl: (cs(k, v), e and ct(e))
    session = Session(secret=settings.SESSION_SECRET_KEY, storage=conn)
elif settings.SESSION_TYPE == "memcache":
    import memcache, time

    conn = memcache.Client(settings.MEMCACHE_CLIENTS, debug=0)
    session = Session(secret=settings.SESSION_SECRET_KEY, storage=conn)
elif settings.SESSION_TYPE == "database":
    from py4web.utils.dbstore import DBStore

    session = Session(secret=settings.SESSION_SECRET_KEY, storage=DBStore(db))

my_auth = Auth(session, db)
my_auth.registration_requires_confirmation = settings.VERIFY_EMAIL

if settings.SMTP_SERVER:
    my_auth.sender = Mailer(
        server=settings.SMTP_SERVER,
        sender=settings.SMTP_SENDER,
        login=settings.SMTP_LOGIN,
        tls=settings.SMTP_TLS,
    )

if my_auth.db:
    groups = Tags(db.auth_user, "groups")

if settings.USE_PAM:
    from py4web.utils.auth_plugins.pam_plugin import PamPlugin

    my_auth.register_plugin(PamPlugin())

if settings.USE_LDAP:
    from py4web.utils.auth_plugins.ldap_plugin import LDAPPlugin

    my_auth.register_plugin(LDAPPlugin(**settings.LDAP_SETTINGS))

if settings.OAUTH2GOOGLE_CLIENT_ID:
    from py4web.utils.auth_plugins.oauth2google import OAuth2Google  # TESTED

    my_auth.register_plugin(
        OAuth2Google(
            client_id=settings.OAUTH2GOOGLE_CLIENT_ID,
            client_secret=settings.OAUTH2GOOGLE_CLIENT_SECRET,
            callback_url="auth/plugin/oauth2google/callback",
        )
    )
if settings.OAUTH2FACEBOOK_CLIENT_ID:
    from py4web.utils.auth_plugins.oauth2facebook import OAuth2Facebook  # UNTESTED

    my_auth.register_plugin(
        OAuth2Facebook(
            client_id=settings.OAUTH2FACEBOOK_CLIENT_ID,
            client_secret=settings.OAUTH2FACEBOOK_CLIENT_SECRET,
            callback_url="auth/plugin/oauth2google/callback",
        )
    )

if settings.USE_CELERY:
    from celery import Celery
    # to use from . common import scheduled and then use it accoding to celery docs
    # examples in tasks.py
    scheduler = Celery('apps.%s.tasks' % settings.APP_NAME, broker=settings.CELERY_BROKER)

# we enable auth, which requres sessions, T, db and we make T available to
# the template, although we recommend client-side translations instead
my_auth.enable(uses=(session, T, db), env=dict(T=T))

unauthenticated = ActionFactory(db, session, T, my_auth)
authenticated = ActionFactory(db, session, T, my_auth.user)
