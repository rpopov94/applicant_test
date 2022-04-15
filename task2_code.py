import os
import logging
import asyncio
import json
from datetime import datetime
from dotenv import load_dotenv


basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))


async def create_tables(cursor):
    tab_1 = asyncio.create_task(cursor.execute("""
            create table if not exists object_status
            (
                object_id INTEGER AUTO_INCREMENT PRIMARY KEY
                occurred_at INTEGER,
                online INTEGER,
                ping INTEGER,
                object VARCHAR(255),
                
            );"""))
    tab_2 = asyncio.create_task(cursor.execute("""
            create table if not exists error_status
            (
                FOREIGN KEY (object_id) REFERENCES error_status(object_id)
                occurred_at INTEGER ,
                object VARCHAR(255),
                errors_tuple JSON,
                object_id VARCHAR(255),
            );"""))
    await asyncio.gather(tab_1, tab_2)


async def accept_status(cursor, **kwargs):
    errors = {}
    ping, online = None, None
    try:
        if kwargs["ping"] < 0:
            errors["ping"] = {"error": "ping < 0"}
        if kwargs["online"] not in (0, 1):
            errors["online"] = {"error": "online not in (0, 1)"}
    except Exception as e:
        errors["parse"] = str(e)
    if errors:
        await cursor.execute("""
                INSERT INTO 
                    public.error_status (occurred_at, object, errors_tuple, object_id) 
                VALUES (?, ?, ?, ?) """,
                             (datetime.now(), kwargs["object"],
                              json.dumps(errors), kwargs["object_id"]))
    else:
        await cursor.execute(f"""
            INSERT INTO 
                public.object_status (occurred_at, object, obj_id, online, ping) 
            VALUES 
                (?, ?, ?, ?, ?);
        """, (datetime.now(), kwargs["object"], kwargs["object_id"], online, ping))
    await cursor.commit()


async def check_token(token):
    if token != os.environ.get('TOKEN'):
        return True
    logging.error("invalid token")
    return False


async def get_statuses(cursor, **kwargs):
    try:
        if await check_token(str(kwargs["TOKEN"])):
            await cursor.execute(
                """
                SELECT occurred_at, online, ping, object, obj_id
                FROM object_status 
                WHERE object = %(_object)s AND obj_id = %(object_id)s
                ORDER BY occurred_at
                """, {'_object': str(kwargs.get("object", "server")),
                      'object_id': int(kwargs["object_id"])})
    except KeyError:
        logging.error("invalid token")
    return cursor.fetchall()


async def get_statuses_errors_by_occurred_at(cursor, **kwargs):
    try:
        if await check_token(str(kwargs["TOKEN"])):
            await cursor.execute("""
                        SELECT 
                            occurred_at, object, errors_tuple
                       FROM error_status WHERE object_id = %(object_id)s
                           AND occurred_at > %(start_at)s AND occurred_at < %(end_at)s
                           AND errors_tuple > %(field)s != '' AND object = %(_object)s
                      ORDER BY occurred_at
                    """, {
                'object_id': int(kwargs["object_id"]),
                'start_at': int(kwargs["start_at"]),
                'end_at:': int(kwargs["end_at"]),
                'field': str(kwargs.get("field", "ping")),
                '_object': str(kwargs.get("object", "server"))
            })
    except KeyError:
        logging.error("invalid token")
    return cursor.fetchall()
