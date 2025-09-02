import logging

from Enums import DBStep


def set_db_step(db, step: DBStep):
    params = db.collection('params')
    params.insert({"_key": "db_step", "value": step.name}, overwrite=True)
    logging.info(f"🔄 db_step updated to: {step.name}")


def get_db_step(db) -> DBStep | None:
    params = db.collection('params')
    if not params.has("db_step"):
        return None
    doc = params.get("db_step")
    return DBStep[doc['value']] if doc else None
