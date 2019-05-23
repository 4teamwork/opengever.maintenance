from __future__ import print_function
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
from plone.app.folder.nogopip import GopipIndex
from Products.ExtendedPathIndex.ExtendedPathIndex import ExtendedPathIndex
from Products.PluginIndexes.BooleanIndex.BooleanIndex import BooleanIndex
from Products.PluginIndexes.DateIndex.DateIndex import DateIndex
from Products.PluginIndexes.DateRangeIndex.DateRangeIndex import DateRangeIndex
from Products.PluginIndexes.FieldIndex.FieldIndex import FieldIndex
from Products.PluginIndexes.KeywordIndex.KeywordIndex import KeywordIndex
from Products.PluginIndexes.UUIDIndex.UUIDIndex import UUIDIndex
from Products.ZCTextIndex.ZCTextIndex import ZCTextIndex
import sys
import transaction

# optional collective.indexing support
try:
    from collective.indexing.queue import processQueue
except ImportError:
    def processQueue():
        pass

# optional Products.DateRecurringIndex support
try:
    from Products.DateRecurringIndex.index import DateRecurringIndex
except ImportError:
    class DateRecurringIndex(object):
        pass


class CantPerformSurgery(Exception):
    """Raised when a procedure can't not be performed."""


class CatalogHealthCheck(object):
    """Run health check for a Products.ZCatalog.Catalog instance.

    Validates that the catalogs uid and rid mapping and metadata is consistent.
    This means that:
    - the mappings have the same length
    - the mappings are consistent, so every item is in the reverse mapping
    - for every item there is also an entry in the catalog metadata

    The health check does not validate indices and index data yet.
    """
    def __init__(self, catalog=None):
        self.portal_catalog = catalog or api.portal.get_tool('portal_catalog')
        self.catalog = self.portal_catalog._catalog

    def run(self):
        result = HealthCheckResult(self.catalog)

        paths = self.catalog.paths
        paths_values = set(self.catalog.paths.values())
        uids = self.catalog.uids
        uids_values = set(self.catalog.uids.values())
        data = self.catalog.data

        uuid_index = self.catalog.indexes['UID']
        result.report_catalog_stats(
            len(self.catalog), len(uids), len(paths), len(data),
            len(uuid_index), len(uuid_index._index), len(uuid_index._unindex))

        for path, rid in uids.items():
            if rid not in paths:
                result.report_symptom(
                    'in_uids_values_not_in_paths_keys', rid, path=path)
            elif paths[rid] != path:
                result.report_symptom(
                    'paths_tuple_mismatches_uids_tuple', rid, path=path)

            if path not in paths_values:
                result.report_symptom(
                    'in_uids_keys_not_in_paths_values', rid, path=path)

            if rid not in data:
                result.report_symptom(
                    'in_uids_values_not_in_metadata_keys', rid, path=path)

        for rid, path in paths.items():
            if path not in uids:
                result.report_symptom(
                    'in_paths_values_not_in_uids_keys', rid, path=path)
            elif uids[path] != rid:
                result.report_symptom(
                    'uids_tuple_mismatches_paths_tuple', rid, path=path)

            if rid not in uids_values:
                result.report_symptom(
                    'in_paths_keys_not_in_uids_values', rid, path=path)

            if rid not in data:
                result.report_symptom(
                    'in_paths_keys_not_in_metadata_keys', rid, path=path)

        for rid in data:
            if rid not in paths:
                result.report_symptom(
                    'in_metadata_keys_not_in_paths_keys', rid)
            if rid not in uids_values:
                result.report_symptom(
                    'in_metadata_keys_not_in_uids_values', rid)

        # we consider the uids (path->rid mapping) as source of truth for the
        # rids "registered" in the catalog. that mapping is also used  in
        # `catalogObject` to decide whether an object is inserted or
        # updated, i.e. if entries for an existing rid are updated or if a
        # new rid is assigned to the path/object.
        rids_in_catalog = uids_values

        index_values = set(uuid_index._index.values())

        for uuid, rid in uuid_index._index.items():
            if rid not in uuid_index._unindex:
                result.report_symptom(
                    'uin_uuid_index_not_in_uuid_unindex', rid)
            elif uuid_index._unindex[rid] != uuid:
                result.report_symptom(
                    'uuuid_index_tuple_mismatches_uuid_unndex_tuple', rid)
            if rid not in rids_in_catalog:
                result.report_symptom(
                    'in_uuid_index_not_in_catalog', rid)

        for rid, uuid in uuid_index._unindex.items():
            if rid not in index_values:
                result.report_symptom(
                    'in_uuid_unindex_not_in_uuid_index', rid)
            if rid not in rids_in_catalog:
                result.report_symptom(
                    'in_uuid_unindex_not_in_catalog', rid)

        return result


class UnhealthyRid(object):
    """Represents a rid which is considered unhealthy.

    A rid becomes unhealthy if the health check finds one or more issues
    with that rid. An `UnhealthyRid` instance groups all issues/symptoms found
    for one rid.

    """
    def __init__(self, rid):
        self.rid = rid
        self._paths = set()
        self._catalog_symptoms = set()

    def attach_path(self, path):
        self._paths.add(path)

    def report_catalog_symptom(self, name):
        """Report a symptom found in the the catalog."""

        self._catalog_symptoms.add(name)

    @property
    def catalog_symptoms(self):
        return tuple(sorted(self._catalog_symptoms))

    @property
    def paths(self):
        return tuple(sorted(self._paths))

    def __str__(self):
        if self.paths:
            paths = ", ".join("'{}'".format(p) for p in self.paths)
        else:
            paths = "--no path--"
        return "rid {} ({})".format(self.rid, paths)

    def write_result(self, formatter):
        formatter.info("{}:".format(self))
        for symptom in self.catalog_symptoms:
            formatter.info('\t- {}'.format(symptom))


class HealthCheckResult(object):
    """Provide health check result for one catalog health check run."""

    def __init__(self, catalog):
        self.catalog = catalog
        self.unhealthy_rids = dict()
        self.claimed_length = None
        self.uids_length = None
        self.paths_length = None
        self.data_length = None
        self.uuid_index_claimed_length = None
        self.uuid_index_index_length = None
        self.uuid_index_unindex_length = None

    def get_unhealthy_rids(self):
        return self.unhealthy_rids.values()

    def report_catalog_stats(self, claimed_length, uids_length, paths_length,
                             data_length,
                             uuid_index_claimed_length,
                             uuid_index_index_length,
                             uuid_index_unindex_length):
        self.claimed_length = claimed_length
        self.uids_length = uids_length
        self.paths_length = paths_length
        self.data_length = data_length
        self.uuid_index_claimed_length = uuid_index_claimed_length
        self.uuid_index_index_length = uuid_index_index_length
        self.uuid_index_unindex_length = uuid_index_unindex_length

    def _make_unhealthy_rid(self, rid, path=None):
        if rid not in self.unhealthy_rids:
            self.unhealthy_rids[rid] = UnhealthyRid(rid)

        unhealthy_rid = self.unhealthy_rids[rid]
        if path:
            unhealthy_rid.attach_path(path)
        return unhealthy_rid

    def report_symptom(self, name, rid, path=None):
        unhealthy_rid = self._make_unhealthy_rid(rid, path=path)
        unhealthy_rid.report_catalog_symptom(name)
        return unhealthy_rid

    def get_symptoms(self, rid):
        return self.unhealthy_rids[rid].catalog_symptoms

    def is_healthy(self):
        """Return whether the catalog is healthy according to this result."""

        return self.is_catalog_data_healthy() and self.is_length_healthy()

    def is_catalog_data_healthy(self):
        return not self.unhealthy_rids

    def is_length_healthy(self):
        return (
            self.claimed_length
            == self.uids_length
            == self.paths_length
            == self.data_length
            == self.uuid_index_claimed_length
            == self.uuid_index_index_length
            == self.uuid_index_unindex_length
        )

    def write_result(self, formatter):
        """Log result to logger."""

        formatter.info("Catalog health check report:")

        if self.is_length_healthy():
            formatter.info(
                "Catalog length is consistent at {}.".format(
                    self.claimed_length))
        else:
            formatter.info("Inconsistent catalog length:")
            formatter.info(" claimed length: {}".format(self.claimed_length))
            formatter.info(" uids length: {}".format(self.uids_length))
            formatter.info(" paths length: {}".format(self.paths_length))
            formatter.info(" metadata length: {}".format(self.data_length))
            formatter.info(" uid index claimed length: {}".format(
                self.uuid_index_claimed_length))
            formatter.info(" uid index index length: {}".format(
                self.uuid_index_index_length))
            formatter.info(" uid index unindex length: {}".format(
                self.uuid_index_unindex_length))

        if self.is_catalog_data_healthy():
            formatter.info("Catalog data is healthy.")
        else:
            formatter.info(
                "Catalog data is unhealthy, found {} unhealthy rids:".format(
                    len(self.unhealthy_rids)))
            for unhealthy_rid in self.unhealthy_rids.values():
                unhealthy_rid.write_result(formatter)
                formatter.info('')


class Surgery(object):
    """Surgery can fix a concrete set of symptoms."""

    def __init__(self, catalog, unhealthy_rid):
        self.catalog = catalog
        self.unhealthy_rid = unhealthy_rid
        self.surgery_log = []

    def perform(self):
        raise NotImplementedError

    def unindex_rid_from_all_catalog_indexes(self, rid):
        for idx in self.catalog.indexes.values():
            if isinstance(idx, GopipIndex):
                # Not a real index
                continue

            if isinstance(idx, (ZCTextIndex, DateRangeIndex,
                                DateRecurringIndex, BooleanIndex)):
                # These are more complex index types, that we don't handle
                # on a low level. We have to hope .unindex_object is able
                # to uncatalog the object and doesn't raise a KeyError.
                idx.unindex_object(rid)
                continue

            if not isinstance(idx, (DateIndex, FieldIndex, KeywordIndex,
                                    ExtendedPathIndex, UUIDIndex)):
                raise CantPerformSurgery(
                    'Unhandled index type: {0!r}'.format(idx))

            entries_pointing_to_rid = [
                val for val, rid_in_index in idx._index.items()
                if rid_in_index == rid]
            if entries_pointing_to_rid:
                # Not quite sure yet if this actually *can* happen
                if len(entries_pointing_to_rid) != 1:
                    raise CantPerformSurgery(
                        'Multiple entries pointing to rid: {}'.format(
                        ' '.join(entries_pointing_to_rid)))
                entry = entries_pointing_to_rid[0]
                del idx._index[entry]

            if rid in idx._unindex:
                del idx._unindex[rid]

            # This should eventually converge to
            # len(_index) == len(_unindex) == _length.value
            actual_min_length = min(len(idx._index), len(idx._unindex))
            length_delta = idx._length.value - actual_min_length
            idx._length.change(length_delta)

        self.surgery_log.append(
            "Removed rid from all catalog indexes.")

    def delete_rid_from_paths(self, rid):
        del self.catalog.paths[rid]

        self.surgery_log.append(
            "Removed rid from paths (the rid->path mapping).")

    def delete_rid_from_metadata(self, rid):
        del self.catalog.data[rid]

        self.surgery_log.append(
            "Removed rid from catalog metadata.")

    def change_catalog_length(self, delta):
        self.catalog._length.change(delta)

    def write_result(self, formatter):
        """Write surgery result to formatter."""

        formatter.info("{}:".format(self.unhealthy_rid))
        for entry in self.surgery_log:
            formatter.info('\t- {}'.format(entry))


class RemoveExtraRid(Surgery):
    """Remove an extra rid from the catalog.

    In this case the object at path still exists but two rids have been
    generated for that object.

    We remove the etra rid from metadata, the rid->path mapping and from
    all indexes.
    """
    def perform(self):
        rid = self.unhealthy_rid.rid
        if len(self.unhealthy_rid.paths) != 1:
            raise CantPerformSurgery(
                "Expected exactly one affected path, got: {}"
                .format(", ".join(self.unhealthy_rid.paths)))

        path = self.unhealthy_rid.paths[0]
        if self.catalog.uids[path] == rid:
            raise CantPerformSurgery(
                "Expected different rid in catalog uids mapping for path {}"
                .format(path))

        self.unindex_rid_from_all_catalog_indexes(rid)
        self.delete_rid_from_paths(rid)
        self.delete_rid_from_metadata(rid)
        self.change_catalog_length(-1)


class RemoveOrphanedRid(Surgery):
    """Remove an orphaned rid from the catalog.

    In this case the object at path no longer exists but the rid still remains
    in the catalog.

    We remove the orphaned rid from metadata, rid->path mapping and from all
    indexes.
    """
    def perform(self):
        rid = self.unhealthy_rid.rid
        if len(self.unhealthy_rid.paths) != 1:
            raise CantPerformSurgery(
                "Expected exactly one affected path, got: {}"
                .format(", ".join(self.unhealthy_rid.paths)))

        path = list(self.unhealthy_rid.paths)[0]
        if path in self.catalog.uids:
            raise CantPerformSurgery(
                "Expected path to be absent from catalog uids {}"
                .format(path))

        portal = api.portal.get()
        obj = portal.unrestrictedTraverse(path, None)
        if obj is not None:
            raise CantPerformSurgery(
                "Unexpectedly found object at {}".format(path))

        self.unindex_rid_from_all_catalog_indexes(rid)
        self.delete_rid_from_paths(rid)
        self.delete_rid_from_metadata(rid)
        self.change_catalog_length(-1)


class CatalogDoctor(object):
    """Performs surgery for an unhealthy_rid, if possible.

    Surgeries are assigned based on symptoms. For each set of symptoms a
    surgical procedure can be registered. This decides if an unhealthy rid can
    be treated.
    """
    surgeries = {
        (
            'in_metadata_keys_not_in_uids_values',
            'in_paths_keys_not_in_uids_values',
            'in_uuid_unindex_not_in_catalog',
            'in_uuid_unindex_not_in_uuid_index',
            'uids_tuple_mismatches_paths_tuple',
        ): RemoveExtraRid,
        (
            'in_metadata_keys_not_in_uids_values',
            'in_paths_keys_not_in_uids_values',
            'in_paths_values_not_in_uids_keys'
        ): RemoveOrphanedRid,
    }

    def __init__(self, catalog, unhealthy_rid):
        self.catalog = catalog
        self.unhealthy_rid = unhealthy_rid

    def can_perform_surgery(self):
        return bool(self.get_surgery())

    def get_surgery(self):
        symptoms = self.unhealthy_rid.catalog_symptoms
        return self.surgeries.get(symptoms, None)

    def perform_surgery(self):
        surgery_cls = self.get_surgery()
        if not surgery_cls:
            return None

        surgery = surgery_cls(self.catalog, self.unhealthy_rid)
        surgery.perform()
        return surgery


class ConsoleOutput(object):

    def info(self, msg):
        print(msg)

    def warning(self, msg):
        print(msg)

    def error(self, msg):
        print(msg, file=sys.stderr)


def healthcheck_command(portal_catalog, formatter):
    result = CatalogHealthCheck(catalog=portal_catalog).run()
    result.write_result(formatter)
    return result


def doctor(portal_catalog):
    formatter = ConsoleOutput()

    result = healthcheck_command(portal_catalog, formatter)
    if result.is_healthy():
        transaction.doom()  # extra paranoia, prevent erroneous commit
        formatter.info('Catalog is healthy, no surgery is needed.')
        return

    there_is_nothing_we_can_do = []
    formatter.info('Performing surgery:')
    for unhealthy_rid in result.get_unhealthy_rids():
        doctor = CatalogDoctor(result.catalog, unhealthy_rid)
        if doctor.can_perform_surgery():
            surgery = doctor.perform_surgery()
            surgery.write_result(formatter)
            formatter.info('')
        else:
            there_is_nothing_we_can_do.append(unhealthy_rid)

    if there_is_nothing_we_can_do:
        formatter.info('The following unhealthy rids could not be fixed:')
        for unhealthy_rid in there_is_nothing_we_can_do:
            unhealthy_rid.write_result(formatter)
            formatter.info('')

    formatter.info('Performing post-surgery healthcheck:')
    post_result = healthcheck_command(portal_catalog, formatter)
    if not post_result.is_healthy():
        transaction.doom()   # extra paranoia, prevent erroneous commit
        formatter.info('Not all health problems could be fixed, aborting.')
        return

    formatter.info('Surgery would have been successful, but was aborted '
                   'due to dryrun!')


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)
    portal_catalog = plone.portal_catalog

    transaction.doom()
    doctor(portal_catalog)
