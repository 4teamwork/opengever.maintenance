"""
Allows to set an arbitrary registry record to a particular value.

The <value> is supposed to be specified as a Python literal.

Example Usage:

    bin/instance run set_registry_value.py <record_dotted_name> <value>
    bin/instance run set_registry_value.py opengever.base.interfaces.IGeverUI.is_feature_enabled True
"""
from ast import literal_eval
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_plone
from plone.registry.interfaces import IRegistry
from zope.component import getUtility
import argparse
import sys
import transaction


def cast_value(value, pytype):
    value = literal_eval(value)
    if isinstance(value, pytype) or value is None:
        return value

    raise ValueError


def set_registry_value(plone, args):
    value = args.value
    record_name = args.record

    registry = getUtility(IRegistry)

    # Locate record
    try:
        record = registry.records[record_name]
    except KeyError:
        raise KeyError("Unable to find registry record %r" % record_name)
    field_type = record.field._type

    # Cast value to record's type
    try:
        value = cast_value(value, field_type)
    except ValueError:
        raise ValueError("Unable to cast value %r to %r for registry "
                         "record %r" % (value, field_type, record_name))

    # Validate and set
    record.field.validate(value)
    record.value = value
    print("Set record %r to %r" % (record_name, value))


if __name__ == "__main__":
    app = setup_app()

    parser = argparse.ArgumentParser()
    parser.add_argument("record", help="Registry record dottedname")
    parser.add_argument("value", help="Value (Python repr syntax)")

    args = parser.parse_args(sys.argv[3:])

    plone = setup_plone(setup_app())

    set_registry_value(plone, args)
    transaction.commit()
