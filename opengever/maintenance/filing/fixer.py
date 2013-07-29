from Products.CMFCore.utils import getToolByName
from StringIO import StringIO
from five import grok
from opengever.base.behaviors.utils import set_attachment_content_disposition
from opengever.dossier.behaviors.dossier import IDossier
from opengever.maintenance.filing.checker import FilingNumberChecker
from xlwt import Workbook, XFStyle
from zope.annotation import IAnnotations
from zope.interface import Interface
from zope.schema.vocabulary import getVocabularyRegistry
import transaction


# excel styles
TITLE_STYLE = XFStyle()
TITLE_STYLE.font.bold = True

OLD_STYLE = XFStyle()
OLD_STYLE.font.colour_index = 0x10
OLD_STYLE.font.outline = False

NEW_STYLE = XFStyle()
NEW_STYLE.font.colour_index = 0x11
NEW_STYLE.font.outline = False

OLD_FILING_NUMBER_KEY = 'old_filing_numbers'


class FakeOptions(object):

    def __init__(self):
        self.verbose = False

FILING_PREFIX_MAPPING = {
    'ska-kr': {'Amt': u'Kantonsrat', },
    'kok-koko': {
        'Finanzdirektion': u'KOM',
        'Kantonsrat': u'KOM'},
    'dbk-ams': {
        u'Direktion f\xfcr Bildung und Kultur': u'Amt',
        u'Regierungsrat': u'Leitung',
    },
    'kom-sko': {
        u'Amt': u'KOM'
    },
    'kom-nlk': {
        u'Amt': u'KOM'
    },
    'kom-stawiko': {
        u'Amt': u'KOM'
    },
    'dbk-aku': {
        u'Direktion f\xfcr Bildung und Kultur': u'Amt',
        u'Regierungsrat': u'Leitung',
    },
    'di-dis': {
        u'Finanzdirektion': u'Direktion',
        u'Direktion des Innern': u'Direktion'
    },
    'ska-ska': {
        u'Staatskanzlei': u'Direktion',
    },
    'bd-bds': {
        u'Baudirektion': u'Direktion',
    },
    'gd-gds': {
        u'Gesundheitsdirektion': u'Direktion',
    },
    'vd-vds':{
        u'Volkswirtschaftsdirektion': u'Direktion',
    },
    'dbk-dbks':{
        u'Direktion f\xfcr Bildung und Kultur': u'Direktion',
        u'Sicherheitsdirektion': u'Direktion',
        u'Gesundheitsdirektion': u'Direktion',
        u'Direktion des Innern': u'Direktion',
        u'Finanzdirektion': u'Direktion',
        u'Kantonsrat': u'Leitung',
    },
    'fd-fds': {
        u'Finanzdirektion': u'Direktion',
    }
}

CLIENT_PREFIX_MAPPING = {
    'di-zibu': {'DI.ZBD': 'DI ZIBU'},

}


class FilingNumberFixer(FilingNumberChecker):

    def __init__(self, options, plone):
        self._fixed_dossiers = {}
        self._fixed_counters = {}
        self._fixed_filing_prefixes = {}

        FilingNumberChecker.__init__(self, options, plone)

    def log_fn_changes(self, path, old, new):
        if self._fixed_dossiers.get(path):
            self._fixed_dossiers[path].append((old, new))
        else:
            self._fixed_dossiers[path] = [(old, new), ]

    def log_counter_changes(self, key, old, new):
        if self._fixed_counters.get(key):
            self._fixed_counters[key].append((old, new))
        else:
            self._fixed_counters[key] = [(old, new), ]

    def log_filing_prefixes_changes(self, path, new):
        self._fixed_filing_prefixes[path] = new

    def fix_legacy_filing_prefixes(self):
        fn_and_paths = self.check_for_legacy_filing_prefixes()

        for fn, path in fn_and_paths:
            fn_parts = fn.split('-')
            new_fn = '%s-%s-%s-%s' % (
                self.current_client_prefix,
                self.legacy_prefixes.get(fn_parts[0]),
                fn_parts[1], fn_parts[2])

            obj = self.plone.unrestrictedTraverse(path.strip('/'))
            self._set_filing_number_without_reindex(obj, new_fn)

            # logging
            self.log_fn_changes(path, fn, new_fn)
            self.log_filing_prefixes_changes(
                path, self.legacy_prefixes.get(fn_parts[0]))

        # check if the fix worked well
        # reset filing_numbers
        self._reset_filing_numbers()

        if len(self.check_for_legacy_filing_prefixes()) > 0:
            raise RuntimeError(
                "The legacy filing prefixes fixer wasn't successfully'"
                ", it exits still some legacy filing prefixes %s" %
                str(self.check_for_legacy_filing_prefixes()))

    def fix_missing_client_prefixes(self, mapping={}):

        fn_and_paths = self.check_for_missing_client_prefixes()

        for fn, path in fn_and_paths:

            # check if the client prefixes is not missing
            # but only a wrong or old one,
            # then we need an equivalent mapping
            if len(fn.split('-')) == 4:
                old_prefix = fn.split('-')[0]
                if not mapping.get(old_prefix):
                    raise AttributeError(
                        "No client prefix mapping is given for: %s" % (
                            old_prefix))
                else:
                    new_fn = fn.replace(old_prefix, mapping.get(old_prefix))

            else:
                new_fn = '%s-%s' % (
                    self.current_client_prefix, fn.lstrip('-'))

                self.log_filing_prefixes_changes(path, new_fn.split('-')[1])

            obj = self.plone.unrestrictedTraverse(path.strip('/'))
            self._set_filing_number_without_reindex(obj, new_fn)

            # logging
            self.log_fn_changes(path, fn, new_fn)


        # check if the fix worked well
        # reset filing_numbers
        self._reset_filing_numbers()

        if len(self.check_for_missing_client_prefixes()) > 0:
            raise RuntimeError(
                "The missing client prefixes fixer wasn't successfully , it "
                "exits still some filing numbers without a client"
                "prefixes %s" % str(
                    self.check_for_missing_client_prefixes()))

    def fix_inexistent_filing_prefixes(self, mapping):

        # get_dossiers with a inexistent filing_prefix
        bad_prefixes = self.check_for_inexistent_filing_prefixes()
        bad_prefixes = [prfx[0] for prfx in bad_prefixes]

        # check if every bad_prefix has now a mapping to a correct one.
        for prfx in bad_prefixes:
            if not mapping.get(prfx, None):
                raise AttributeError(
                    u'Missing prefix mapping for bad prefix: %s' % (prfx))

        # get all dossiers wich should be fixed
        to_fix = []
        for prefix in bad_prefixes:
            to_fix += self.get_associated_to_filing_prefix_numbers(prefix)

        # fix
        for fn, path, prefix in to_fix:
            obj = self.plone.unrestrictedTraverse(path.strip('/'))
            new_fn = fn.replace(prefix, mapping.get(prefix))
            self._set_filing_number_without_reindex(obj, new_fn)

            # logging
            self.log_fn_changes(path, fn, new_fn)
            self.log_filing_prefixes_changes(path, new_fn.split('-')[1])

        # check if the fix worked well
        # reset filing_numbers
        self._reset_filing_numbers()

        if len(self.check_for_inexistent_filing_prefixes()) > 0:
            raise RuntimeError(
                "The inexistent_filing_prefix fixer wasn't successfully'"
                ", it exits still some inexistent_filing_prefix %s" %
                str(self.check_for_inexistent_filing_prefixes()))

    def fix_duplicates(self):
        """Method wichi Fix all existing duplicates
        a fix for fuzzy duplicates isn't necessary, because
        they should allready fixed with the
        inexistent filing prefixes fixer.
        """

        # get dossiers with duplicates
        duplicates = self.check_for_duplicates()

        duplicate_mapping = {}
        for fn, path in duplicates:
            if duplicate_mapping.get(fn, None):
                duplicate_mapping[fn].append(path)
            else:
                duplicate_mapping[fn] = [path, ]

        for fn, paths in duplicate_mapping.items():
            # get_all_objs with the same fn
            objs = [self.plone.unrestrictedTraverse(path.strip('/'))
                    for path in paths]

            # sort on created date
            objs = sorted(objs, key=lambda obj: obj.created(), reverse=False)

            # give a new filing number for every duplicate
            for obj in objs[1:]:
                new_fn = self.set_next_filing_number(obj)

                # logging
                self.log_fn_changes(
                    '/'.join(obj.getPhysicalPath()), fn, new_fn)

        # check if the fix worked well
        # reset filing_numbers
        self._reset_filing_numbers()
        if len(self.check_for_duplicates()) > 0:
            raise RuntimeError(
                "The duplicates fixer wasn't successfully'"
                ", it exits still some duplicated filing numbers %s" %
                str(self.check_for_duplicates()))

        # also all fuzzy duplicates should be solved
        if len(self.check_for_fuzzy_duplicates()) > 0:
            raise RuntimeError(
                "The duplicates fixer wasn't successfully'"
                ", it exits still some FUZZY(!) duplicated filing numbers %s" %
                str(self.check_for_fuzzy_duplicates()))

    def fix_bad_counters(self):
        bad_counters = self.check_for_bad_counters()

        for key, old_value, highest_fn in bad_counters:
            new_value = self.get_number_part(highest_fn)
            self.set_counter_value(key, new_value)

            # logging
            self.log_counter_changes(key, old_value, new_value)

        # check if the fix
        if len(self.check_for_bad_counters()) > 0:
            raise RuntimeError(
                "The bad counters fixer wasn't successfully'"
                ", it exits still some bad counters %s" %
                str(self.check_for_duplicates()))

    def fix_counters_needing_initialization(self):
        counters = self.check_for_counters_needing_initialization()

        for counter_key, numbers in counters:
            associated_fns = self.get_associated_filing_numbers(counter_key)
            highest_fn = self.get_highest_filing_number(associated_fns)
            new_value = self.get_number_part(highest_fn)
            self.create_counter(counter_key, new_value)

            # logging
            self.log_counter_changes(counter_key, 'NONE', new_value)

        # check if the fix
        if len(self.check_for_counters_needing_initialization()) > 0:
            raise RuntimeError(
                "The counters who need a initialization fixer "
                "wasn't successfully', the following counters still need "
                "an initialization %s" % str(
                    self.check_for_counters_needing_initialization()))

    def fix_dotted_client_prefixes(self):
        fns_and_paths = self.check_for_dotted_client_prefixes()

        dotted_prefix = self.current_client_prefix.replace(' ', '.')
        current_prefix = self.current_client_prefix

        for fn, path in fns_and_paths:
            obj = self.plone.unrestrictedTraverse(path.strip('/'))
            new_fn = fn.replace(dotted_prefix, current_prefix)
            self._set_filing_number_without_reindex(obj, new_fn)

            # logging
            self.log_fn_changes(path, fn, new_fn)

        # check if the fix worked well
        # reset filing_numbers
        self._reset_filing_numbers()
        if len(self.check_for_dotted_client_prefixes()) > 0:
            raise RuntimeError(
                "The dotted client prefixes fixer wasn't successfully"
                ", the following fn still have an dotten client prefix "
                "%s." % str(self.check_for_dotted_client_prefixes()))

    def fix_invalid_filing_numbers(self):
        fns_and_paths = self.check_for_invalid_filing_numbers()

        for fn, path in fns_and_paths:
            obj = self.plone.unrestrictedTraverse(path.strip('/'))
            new_fn = None
            self._set_filing_number_without_reindex(obj, new_fn)

            self.log_fn_changes(path, fn, '--REMOVED--')

        # check if the fix worked well
        self._reset_filing_numbers()
        if len(self.check_for_invalid_filing_numbers()):
            raise RuntimeError(
                "The invalid filing numbers (format) wasn't sucessfully"
                ", the following filling numbers ar still invalid %s." % str(
                    self.check_for_invalid_filing_numbers()))


class FixFilingNumbers(grok.View):
    grok.name('fix_filing')
    grok.context(Interface)

    def render(self):

        transaction.doom()

        fixer = FilingNumberFixer(FakeOptions(), self.context)
        fixer.run()

        client_id = getToolByName(
            self.context, 'portal_url').getPortalObject().getId()

        # fix dotted clients
        fixer.fix_dotted_client_prefixes()
        # legacy filing prefixes
        fixer.fix_legacy_filing_prefixes()
        # missing client prefix
        fixer.fix_missing_client_prefixes(
            mapping=CLIENT_PREFIX_MAPPING.get(client_id, {}))
        # bad counters
        fixer.fix_bad_counters()
        # inexistent filing prefix
        fixer.fix_inexistent_filing_prefixes(
            FILING_PREFIX_MAPPING.get(client_id, {}))
        # reset filing numbers without the right format
        fixer.fix_invalid_filing_numbers()
        # counters could be bad again so we have to check or fix them again.
        fixer.fix_bad_counters()
        # counters need initialization
        fixer.fix_counters_needing_initialization()
        # duplicates
        fixer.fix_duplicates()

        self.update_filing_prefixes(fixer._fixed_filing_prefixes)

        self.store_old_numbers(fixer._fixed_dossiers)

        # all fixers done
        checker = FilingNumberChecker(FakeOptions(), self.context)
        checker.run()
        if len([k for k, v in checker.results.items() if len(v) > 0]):
            raise RuntimeError(
                'All fixes done, but the Checker still detected '
                'some problems %s' % (str(checker.results)))

        data = self.generate_excel(
            fixer._fixed_dossiers,
            fixer.get_filing_number_counters(),
            fixer._fixed_filing_prefixes)
        response = self.request.RESPONSE

        response.setHeader('Content-Type', 'application/vnd.ms-excel')
        set_attachment_content_disposition(
            self.request, "dossier_report.xls")

        return data

    def store_old_numbers(self, fixed_dossiers):
        """store old filing number in the annotation of the obj
        """

        for path, numbers in fixed_dossiers.items():
            obj = self.context.unrestrictedTraverse(path)
            ann = IAnnotations(obj)
            ann[OLD_FILING_NUMBER_KEY] = numbers[0][0]

    def update_filing_prefixes(self, fixed_dossiers):
        for path, prefix in fixed_dossiers.items():
            obj = self.context.unrestrictedTraverse(path)
            IDossier(obj).filing_prefix = self.get_prefix_value(obj, prefix)
            obj.reindexObject()

    def get_prefix_value(self, obj, prefix):
        if not hasattr(self, 'prefix_vocabulary'):
            voca = getVocabularyRegistry().get(obj, 'opengever.dossier.type_prefixes')
            self.prefix_vocabulary = {}
            for value, term in voca.by_value.items():
                self.prefix_vocabulary[term.title] = value

        return self.prefix_vocabulary.get(prefix)

    def generate_excel(self, fixed_dossiers, counters, fixed_prefixes):
        w = Workbook()

        self._add_dossier_sheet(w, fixed_dossiers)
        self._add_counters_sheet(w, counters)
        self._add_prefix_sheet(w, fixed_prefixes)
        data = StringIO()
        w.save(data)
        data.seek(0)

        return data.read()

    def _add_dossier_sheet(self, w, dossiers):
        sheet = w.add_sheet('changed dossiers')

        for r, path in enumerate(dossiers.keys()):
            sheet.write(r, 0, path, TITLE_STYLE)

            counter = 1
            for fn_pair in dossiers.get(path):
                sheet.write(r, counter, fn_pair[0], OLD_STYLE)
                sheet.write(r, counter + 1, fn_pair[1], NEW_STYLE)
                counter += 2

        # set_size
        sheet.col(0).width = 17500
        for i in range(1, 16):
            sheet.col(i).width = 5000

    def _add_counters_sheet(self, w, counters):
        sheet = w.add_sheet('counters overview')
        for r, key in enumerate(counters.keys()):
            sheet.write(r, 0, key, TITLE_STYLE)
            sheet.write(r, 1, counters.get(key).value)

        # set_size
        sheet.col(0).width = 5000
        sheet.col(1).width = 5000

    def _add_prefix_sheet(self, w, prefixes):
        sheet = w.add_sheet('changed prefixes')

        for r, path in enumerate(prefixes.keys()):
            sheet.write(r, 0, path, TITLE_STYLE)
            sheet.write(r, 1, prefixes.get(path), NEW_STYLE)

        # set_size
        sheet.col(0).width = 17500
        for i in range(1, 16):
            sheet.col(i).width = 5000
