from copy import copy
from plone import api
from Products.Five.browser import BrowserView
from zope.globalrequest import getRequest
from zope.i18n import translate
from zope.interface import Interface


try:
    from opengever.meeting import _
    from opengever.meeting.command import ExcerptOperations
    from opengever.meeting.command import UpdateGeneratedDocumentCommand
    from opengever.meeting.interfaces import IMeetingWrapper

except ImportError:
    class IMeetingWrapper(Interface):
        """Mock interface to make imports happy."""


class ReGenerateExcerpts(BrowserView):
    """Add a view to re-generate excerpts for a meeting.
    """

    def __init__(self, context, request):
        super(ReGenerateExcerpts, self).__init__(context, request)
        self.repository = api.portal.get_tool('portal_repository')

    def __call__(self):
        return self.re_generate_excerpts()

    def re_generate_excerpts(self):
        """Re-generate excerpts for a closed meeting.

        - Updates excerpt in meeting/submitted proposal
        - Updates excerpt in proposal's dossier

        """
        meeting = self.context.model
        if meeting.workflow_state != 'closed':
            return 'meeting is not closed, view cannot be run.'

        for agenda_item in meeting.agenda_items:
            if not agenda_item.has_proposal:
                continue

            self.update_document_in_meeting(agenda_item)
            self.update_document_in_dossier(agenda_item)

        return 'done'

    def update_document_in_meeting(self, agenda_item):
        proposal = agenda_item.proposal
        generated_doc = proposal.submitted_excerpt_document
        generated_doc.meeting = agenda_item.meeting  # temporarily remember meeting

        operations = ExcerptOperations([agenda_item])
        UpdateGeneratedDocumentCommand(generated_doc, operations).execute()

    def update_document_in_dossier(self, agenda_item):
        proposal = agenda_item.proposal
        meeting = agenda_item.meeting

        # excerpt in meeting/committee
        submitted_excerpt = proposal.resolve_submitted_excerpt_document()
        # excerpt in dossier
        excerpt = proposal.resolve_excerpt_document()

        # copy updated file data from meeting to dossier
        excerpt.file.data = copy(submitted_excerpt.file.data)
        comment = translate(
            _(u'Updated with a newer generated version from meeting ${title}.',
              mapping=dict(title=meeting.get_title())),
            context=getRequest())
        self.repository.save(obj=excerpt, comment=comment)

        # update version in sql
        new_version = excerpt.get_current_version()
        proposal.excerpt_document.generated_version = new_version
