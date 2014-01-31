"""Helpers for dealing with documents in the context of PDF conversion.
"""


def has_file(doc):
    return doc.file is not None


def is_pdf(doc):
    return doc.file.contentType == 'application/pdf'


def in_status(status_list):
    if isinstance(status_list, basestring):
        status_list = [status_list]

    def status_checker(doc):
        return get_status(doc) in status_list

    return status_checker


def get_status(doc):
    from opengever.pdfconverter.behaviors.preview import IPreview
    from opengever.pdfconverter.behaviors.preview import CONVERSION_STATE_READY
    from opengever.pdfconverter.behaviors.preview import CONVERSION_STATE_FAILED
    from opengever.pdfconverter.behaviors.preview import CONVERSION_STATE_CONVERTING

    STATUS_MAP = {
        None: 'not_converted',
        CONVERSION_STATE_CONVERTING: 'converting',
        CONVERSION_STATE_FAILED: 'failed',
        CONVERSION_STATE_READY: 'converted'
    }
    status = IPreview(doc).conversion_state
    return STATUS_MAP[status]


class DocumentCollector(object):
    """Helper to collect different (sub-)sets of documents in the context of
    PDF conversion / listing conversion statuses.

    A list of all documents is built when this class is instanciated, and
    all public methods are memoized for effiency.
    """

    def __init__(self, site):
        self.site = site

        doc_brains = self.site.portal_catalog(
            portal_type='opengever.document.document')
        self._all_docs = [brain.getObject() for brain in doc_brains]

        self._docs_with_file = None
        self._docs_without_file = None
        self._pdf_docs = None
        self._non_pdf_docs = None
        self._converted_docs = None
        self._converting_docs = None
        self._failed_docs = None
        self._not_converted_docs = None
        self._docs_missing_pdf = None

    def all_docs(self):
        """Returns all objects of type `opengever.document.document`.
        """
        return set(self._all_docs)

    def docs_with_file(self):
        """Returns all documents that have a file.
        """
        if not self._docs_with_file:
            self._docs_with_file = filter(has_file, self._all_docs)
        return set(self._docs_with_file)

    def docs_without_file(self):
        """Returns all documents that don't have a file.
        """
        if not self._docs_without_file:
            self._docs_without_file = self.all_docs() - self.docs_with_file()
        return set(self._docs_without_file)

    def pdf_docs(self):
        """Returns all documents whose file is a PDF.
        """
        if not self._pdf_docs:
            self._pdf_docs = filter(is_pdf, self.docs_with_file())
        return set(self._pdf_docs)

    def non_pdf_docs(self):
        """Returns all documents that have a file that is not a PDF.
        """
        if not self._non_pdf_docs:
            self._non_pdf_docs = self.docs_with_file() - self.pdf_docs()
        return set(self._non_pdf_docs)

    # The following methods only return documents that are candidates
    # for conversion (they have a file, and the file is not a PDF).

    def converted_docs(self):
        """Returns all non-PDF documents in status `converted`.
        """
        if not self._converted_docs:
            self._converted_docs = filter(in_status('converted'),
                                          self.non_pdf_docs())
        return set(self._converted_docs)

    def converting_docs(self):
        """Returns all non-PDF documents in status `converting`.
        """
        if not self._converting_docs:
            self._converting_docs = filter(in_status('converting'),
                                          self.non_pdf_docs())
        return set(self._converting_docs)

    def failed_docs(self):
        """Returns all non-PDF documents in status `failed`.
        """
        if not self._failed_docs:
            self._failed_docs = filter(in_status('failed'),
                                          self.non_pdf_docs())
        return set(self._failed_docs)

    def not_converted_docs(self):
        """Returns all non-PDF documents in status `not_converted`.
        """
        if not self._not_converted_docs:
            self._not_converted_docs = filter(in_status('not_converted'),
                                          self.non_pdf_docs())
        return set(self._not_converted_docs)

    def docs_missing_pdf(self):
        """Returns all documents that should have a preview PDF, but don't.
        """
        if not self._docs_missing_pdf:
            self._docs_missing_pdf = filter(
                in_status(['converting', 'failed', 'not_converted']),
                self.non_pdf_docs())
        return self._docs_missing_pdf


def reset_conversion_status(docs):
    """Helper function to reset conversion status to None ('not_converted') for
    a list of documents.
    """
    from opengever.pdfconverter.behaviors.preview import IPreview
    for doc in docs:
        IPreview(doc).conversion_state = None
