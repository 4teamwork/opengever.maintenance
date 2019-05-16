from __future__ import print_function
from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import sys
import transaction


class Surgery(object):
    """Surgery can fix a concrete set of symptoms."""

    def __init__(self, catalog, unhealthy_rid):
        self.catalog = catalog
        self.unhealthy_rid = unhealthy_rid

    def perform(self):
        raise NotImplementedError

    def unindex_rid_from_all_catalog_indexes(self, rid):
        for index in self.catalog.indexes.values():
            index.unindex_object(rid)  # fail in case of no `unindex_object`

    def delete_rid_from_paths(self, rid):
        del self.catalog.paths[rid]

    def delete_rid_from_metadata(self, rid):
        del self.catalog.data[rid]

    def change_catalog_length(self, delta):
        self.catalog._length.change(delta)


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

        path = list(self.unhealthy_rid.paths)[0]
        if self.catalog.uids[path] == rid:
            raise CantPerformSurgery(
                "Expected different rid in catalog uids mapping for path {}"
                .format(path))

        self.unindex_rid_from_all_catalog_indexes(rid)
        self.delete_rid_from_paths(rid)
        self.delete_rid_from_metadata(rid)
        self.change_catalog_length(-1)

        return "Removed {} from catalog.".format(rid)


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

        return "Removed {} from catalog.".format(rid)


class CatalogDoctor(object):
    """Performs surgery for an unhealthy_rid, if possible.

    Surgeries are assigned based on symptoms. For each set of symptoms a
    surgical procedure can be registered. This decides if an unhealthy rid can
    be treated.
    """
    surgeries = {
        ('in_metadata_keys_not_in_uids_values',
         'in_paths_keys_not_in_uids_values',
         'uids_tuple_mismatches_paths_tuple',
         ): RemoveExtraRid,
        ('in_metadata_keys_not_in_uids_values',
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
        surgery = self.get_surgery()
        if not surgery:
            return None

        return surgery(self.catalog, self.unhealthy_rid).perform()


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

        result.report_catalog_stats(
            len(self.catalog), len(uids), len(paths), len(data))

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

        return result


class UnhealthyRid(object):
    """Represents a rid which is considered unhealthy.

    A rid becomes unhealthy if the health check finds one or more issues
    with that rid. An `UnhealthyRid` instance groups all issues/symptoms found
    for one rid.

    """
    def __init__(self, rid):
        self.rid = rid
        self.paths = set()
        self._catalog_symptoms = set()

    def attach_path(self, path):
        self.paths.add(path)

    def report_catalog_symptom(self, name):
        """Report a symptom found in the the catalog."""

        self._catalog_symptoms.add(name)

    @property
    def catalog_symptoms(self):
        return tuple(sorted(self._catalog_symptoms))

    def write_result(self, formatter):
        if self.paths:
            paths = ", ".join("'{}'".format(p) for p in self.paths)
        else:
            paths = "--no path--"
        formatter.info("rid: {} ({})".format(self.rid, paths))
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

    def get_unhealthy_rids(self):
        return self.unhealthy_rids.values()

    def report_catalog_stats(self, claimed_length, uids_length, paths_length, data_length):
        self.claimed_length = claimed_length
        self.uids_length = uids_length
        self.paths_length = paths_length
        self.data_length = data_length

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

        return self.is_index_data_healthy() and self.is_length_healthy()

    def is_index_data_healthy(self):
        return not self.unhealthy_rids

    def is_length_healthy(self):
        return (
            self.claimed_length
            == self.uids_length
            == self.paths_length
            == self.data_length
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

        if self.is_index_data_healthy():
            formatter.info("Index data is healthy.")
        else:
            formatter.info(
                "Index data is unhealthy, found {} unhealthy rids:".format(
                    len(self.unhealthy_rids)))
            for unhealthy_rid in self.unhealthy_rids.values():
                unhealthy_rid.write_result(formatter)
                formatter.info('')


class ConsoleOutput(object):

    def info(self, msg):
        print(msg)

    def warning(self, msg):
        print(msg)

    def error(self, msg):
        print(msg, file=sys.stderr)


if __name__ == '__main__':
    app = setup_app()

    parser = setup_option_parser()
    (options, args) = parser.parse_args()

    plone = setup_plone(app, options)

    transaction.doom()

    result = CatalogHealthCheck(catalog=plone.portal_catalog).run()
    result.write_result(formatter=ConsoleOutput())

    formatter = ConsoleOutput()
    formatter.info('Performing surgery:')
    there_is_nothing_we_can_do = []

    for unhealthy_rid in result.get_unhealthy_rids():
        doctor = CatalogDoctor(result.catalog, unhealthy_rid)
        if doctor.can_perform_surgery():
            formatter.info(doctor.perform_surgery())
        else:
            there_is_nothing_we_can_do.append(unhealthy_rid)

    if there_is_nothing_we_can_do:
        formatter.info('The following unhealthy rids could not be fixed')
        for unhealthy_rid in there_is_nothing_we_can_do:
            unhealthy_rid.write_result(formatter)

    formatter.info('')
    formatter.info('Performing post-surgery checkup:')
    result_post = CatalogHealthCheck(catalog=plone.portal_catalog).run()
    result_post.write_result(formatter=ConsoleOutput())
