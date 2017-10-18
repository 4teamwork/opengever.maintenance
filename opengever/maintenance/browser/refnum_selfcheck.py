from Acquisition import aq_inner
from Acquisition import aq_parent
from opengever.base.interfaces import IReferenceNumber
from opengever.base.interfaces import IReferenceNumberPrefix
from opengever.dossier.behaviors.dossier import IDossierMarker
from opengever.dossier.templatedossier import ITemplateDossier
from opengever.repository.interfaces import IRepositoryFolder
from opengever.repository.repositoryroot import IRepositoryRoot
from persistent.dict import PersistentDict
from persistent.list import PersistentList
from plone.registry.interfaces import IRegistry
from Products.Five.browser import BrowserView
from zope.annotation.interfaces import IAnnotations
from zope.app.intid.interfaces import IIntIds
from zope.component import getAdapter
from zope.component import getUtility
from zope.component import queryAdapter
import transaction


try:
    from opengever.base.adapters import CHILD_REF_KEY
    from opengever.base.adapters import DOSSIER_KEY
    from opengever.base.adapters import REPOSITORY_FOLDER_KEY
    from opengever.base.adapters import PREFIX_REF_KEY
    OLD_CODE_BASE = False
except ImportError:
    OLD_CODE_BASE = True
    from opengever.base.adapters import ReferenceNumberPrefixAdpater
    CHILD_REF_KEY = ReferenceNumberPrefixAdpater.CHILD_REF_KEY
    PREFIX_REF_KEY = ReferenceNumberPrefixAdpater.REF_KEY
    DOSSIER_KEY = None
    REPOSITORY_FOLDER_KEY = None


try:
    from opengever.base.interfaces import IReferenceNumberFormatter
    from opengever.base.interfaces import IReferenceNumberSettings
    REFNUM_FORMATTER_AVAILABLE = True
except ImportError:
    REFNUM_FORMATTER_AVAILABLE = False


class RefnumSelfcheckView(BrowserView):
    """A view to run self-checks on reference numbers.
    """

    def log(self, msg):
        self.request.response.write(msg + "\n")

    def __call__(self):
        transaction.doom()

        site = self.context
        checker = ReferenceNumberChecker(self.log, site)
        checker.selfcheck()


class ReferenceNumberHelper(object):
    """Helper class for dealing with reference numbers.
    """

    def __init__(self, log_func, site):
        self.log = log_func
        self.site = site

    def get_repo_dossier_separator(self, obj=None):
        if OLD_CODE_BASE:
            return '/'
        else:
            if REFNUM_FORMATTER_AVAILABLE:
                registry = getUtility(IRegistry)
                proxy = registry.forInterface(IReferenceNumberSettings)

                formatter = queryAdapter(obj,
                                         IReferenceNumberFormatter,
                                         name=proxy.formatter)
                return formatter.repository_dossier_seperator
            else:
                return '/'

    def get_new_mapping(self, key, obj):
        parent = aq_parent(aq_inner(obj))
        ann = IAnnotations(parent)

        if IDossierMarker.providedBy(obj):
            mapping_base = ann.get(DOSSIER_KEY)
        elif IRepositoryFolder.providedBy(obj) or IRepositoryRoot.providedBy(obj):
            mapping_base = ann.get(REPOSITORY_FOLDER_KEY)
        else:
            raise Exception("Unknown object type!")

        if not mapping_base:
            return None

        mapping = mapping_base.get(key)
        return mapping


class ReferenceNumberChecker(object):
    """Various checks to validate reference number integrity.
    """

    def __init__(self, log_func, site):
        self.parent_logger = log_func
        self.site = site
        self.helper = ReferenceNumberHelper(log_func, site)
        self.intids = getUtility(IIntIds)
        self.ignored_ids = ['vorlagen']

    def log(self, msg):
        msg = "    " + msg
        return self.parent_logger(msg)

    def selfcheck(self):
        self.log("Running reference number self-checks...")

        checks = ('check_if_dossier_refnums_are_complete',
                  'check_for_duplicate_refnums',
                  'check_if_index_equals_objdata',
                  'check_if_in_proper_mappings',
                  'check_if_mappings_are_persistent')

        results = {}
        for checkname in checks:
            self.log("Running '{}'...".format(checkname))
            result = getattr(self, checkname)()
            self.log("Done {}: {}".format(checkname, result))
            results[checkname] = result

        return results

    def check_if_dossier_refnums_are_complete(self):
        check_result = 'PASSED'
        catalog = self.site.portal_catalog
        dossier_brains = catalog(object_provides=IDossierMarker.__identifier__)

        for brain in dossier_brains:
            dossier = brain.getObject()
            sep = self.helper.get_repo_dossier_separator(obj=dossier)
            url = dossier.absolute_url()

            if dossier.id in self.ignored_ids:
                continue
            if ITemplateDossier.providedBy(dossier):
                continue

            refNumb = getAdapter(dossier, IReferenceNumber)
            obj_refnum = refNumb.get_number()
            refnum_parts = obj_refnum.split(sep)
            if '/desktop/' in url:
                # Dossiers in Desktop position have a custom reference number
                continue

            if not len(refnum_parts) == 2:
                check_result = 'FAILED'
                self.log("WARNING: Something's wrong with "
                         "refnum '%s' for object '%s'"
                    % (obj_refnum, url))
                continue

            right_part = refnum_parts[1].strip()
            left_part = refnum_parts[0].strip()
            if right_part.endswith('.') or right_part.startswith('.') \
                or left_part.endswith('.') or left_part.startswith('.'):
                check_result = 'FAILED'
                self.log("WARNING: refnum '%s' for object '%s' is "
                         "incomplete!" % (obj_refnum, url))
                continue

        return check_result

    def check_for_duplicate_refnums(self):
        check_result = 'PASSED'
        all_refnums = dict()

        catalog = self.site.portal_catalog

        dossier_brains = catalog(object_provides=IDossierMarker.__identifier__)
        repo_brains = catalog(object_provides=IRepositoryFolder.__identifier__)

        all_brains = []
        all_brains.extend(dossier_brains)
        all_brains.extend(repo_brains)

        for brain in all_brains:
            obj = brain.getObject()
            obj_path = '/'.join(obj.getPhysicalPath())

            if '/desktop/' in obj_path:
                # Dossiers in Desktop position have a custom reference number
                continue

            reporoot_prefix = '/'.join(obj_path.split('/')[:3])

            refNumb = getAdapter(obj, IReferenceNumber)
            obj_refnum = refNumb.get_number()

            if obj_refnum in all_refnums.values():
                dups = [path for path, rn in all_refnums.items()
                        if obj_refnum == rn]
                dups = filter(lambda path: path.startswith(
                    reporoot_prefix), dups)
                if dups:
                    check_result = 'FAILED'
                    self.log("WARNING: Reference Number for object "
                             "'%s' is a duplicate!" % obj.id)
                    self.log("Object: %s" % obj.absolute_url())
                    self.log("RefNum: %s" % obj_refnum)
                    self.log("Duplicates:")
                    for dup in dups:
                        self.log(dup)
                    self.log("")

            all_refnums[obj_path] = obj_refnum
        return check_result

    def check_if_index_equals_objdata(self):
        check_result = 'PASSED'
        catalog = self.site.portal_catalog
        dossier_brains = catalog(object_provides=IDossierMarker.__identifier__)

        for brain in dossier_brains:
            dossier = brain.getObject()

            if dossier.absolute_url().endswith('vorlagen'):
                continue

            refNumb = getAdapter(dossier, IReferenceNumber)
            obj_refnum = refNumb.get_number()

            if not brain.reference == obj_refnum:
                check_result = 'FAILED'
                self.log("WARNING: ReferenceNumber for Dossier '%s' differs "
                    "from value in catalog metadata!" % dossier.absolute_url())
                msg_objvalue = ("Object: %s" % obj_refnum).ljust(40)
                msg_idxvalue = ("Metadata: %s" % brain.reference).ljust(40)
                self.log("%s %s" % (msg_objvalue, msg_idxvalue))
                self.log("")
        return check_result

    def _check_if_in_new_mappings(self, obj):
        """Check whether `obj` is in both new-style mappings of its parent.
        """
        check_result = 'PASSED'
        parent = aq_parent(aq_inner(obj))
        local_number = IReferenceNumberPrefix(parent).get_number(obj)
        intid = self.intids.getId(obj)
        try:
            child_mapping = self.helper.get_new_mapping(CHILD_REF_KEY, obj)
            if not child_mapping[local_number] == intid:
                check_result = 'FAILED'
                self.log("WARNING: obj %s not in child mapping of parent!" % obj)

            prefix_mapping = self.helper.get_new_mapping(PREFIX_REF_KEY, obj)
            if not prefix_mapping[intid] == local_number:
                check_result = 'FAILED'
                self.log("WARNING: obj %s not in prefix mapping of parent!" % obj)
        except Exception, e:
            check_result = 'FAILED'
            self.log("WARNING: '%s' for %s" % (e, obj))

        return check_result

    def _check_if_in_old_mappings(self, obj):
        """Check whether `obj` is in both old-style mappings of its parent.
        """
        check_result = 'PASSED'
        parent = aq_parent(aq_inner(obj))
        local_number = IReferenceNumberPrefix(parent).get_number(obj)
        intid = self.intids.getId(obj)
        ann = IAnnotations(parent)

        try:
            child_mapping = ann.get(CHILD_REF_KEY)
            if not child_mapping[local_number] == intid:
                check_result = 'FAILED'
                self.log("WARNING: obj %s not in child mapping of parent!" % obj)

            prefix_mapping = ann.get(PREFIX_REF_KEY)
            if not prefix_mapping[intid] == local_number:
                check_result = 'FAILED'
                self.log("WARNING: obj %s not in prefix mapping of parent!" % obj)
        except Exception, e:
            check_result = 'FAILED'
            self.log("WARNING: '%s' for %s" % (e, obj))

        return check_result

    def check_if_in_proper_mappings(self):
        check_result = 'PASSED'
        catalog = self.site.portal_catalog

        dossier_brains = catalog(object_provides=IDossierMarker.__identifier__)
        repo_brains = catalog(object_provides=IRepositoryFolder.__identifier__)

        all_brains = []
        all_brains.extend(dossier_brains)
        all_brains.extend(repo_brains)

        for brain in all_brains:
            obj = brain.getObject()

            # Skip ignored objects
            if obj.id in self.ignored_ids \
                or obj.portal_type == 'opengever.phvs.homefolder':
                continue

            if OLD_CODE_BASE:
                result = self._check_if_in_old_mappings(obj)
            else:
                result = self._check_if_in_new_mappings(obj)
            if not result == 'PASSED':
                check_result = 'FAILED'
        return check_result

    def check_if_mappings_are_persistent(self):
        check_result = 'PASSED'
        catalog = self.site.portal_catalog

        dossier_brains = catalog(object_provides=IDossierMarker.__identifier__)
        repo_brains = catalog(object_provides=IRepositoryFolder.__identifier__)

        all_brains = []
        all_brains.extend(dossier_brains)
        all_brains.extend(repo_brains)

        for brain in all_brains:
            obj = brain.getObject()
            url = obj.absolute_url()
            ann = IAnnotations(obj)

            if OLD_CODE_BASE:
                child_refs = ann.get(CHILD_REF_KEY)
                prefix_refs = ann.get(PREFIX_REF_KEY)
            else:
                child_refs = self.helper.get_new_mapping(CHILD_REF_KEY, obj)
                prefix_refs = self.helper.get_new_mapping(PREFIX_REF_KEY, obj)

            if child_refs:
                child_refs_persistent = is_persistent(child_refs)
                if not child_refs_persistent:
                    check_result = 'FAILED'
                    self.log("FAILED: child refs not persistent for %s" % url)

            if prefix_refs:
                prefix_refs_persistent = is_persistent(prefix_refs)
                if not prefix_refs_persistent:
                    check_result = 'FAILED'
                    self.log("FAILED: prefix refs not persistent for %s" % url)
        return check_result


def instance_of(obj, types):
    """Checks if item is an instance of any of the types.
    """
    return any([isinstance(obj, t) for t in types])


def is_persistent(thing):
    """Recursive function that checks if a structure containing nested lists
    and dicts uses the Persistent types all the way down.
    """
    if not instance_of(thing, [list, dict]):
        # It's neither a subclass of list or dict, so it's fine
        return True

    if not instance_of(thing, [PersistentList, PersistentDict]):
        # It's a subclass of list or dict, but not a persistent one - bad!
        return False

    if isinstance(thing, PersistentList):
        return all([is_persistent(i) for i in thing])

    elif isinstance(thing, PersistentDict):
        return all([is_persistent(v) for v in thing.values()])
