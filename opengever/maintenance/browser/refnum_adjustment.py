from Acquisition import aq_base
from Acquisition import aq_inner
from Acquisition import aq_parent
from opengever.base import _
from opengever.base.interfaces import IReferenceNumber
from opengever.base.interfaces import IReferenceNumberPrefix
from opengever.repository.behaviors.referenceprefix import IReferenceNumberPrefix as IReferenceNumberPrefixBehavior
from opengever.repository.interfaces import IRepositoryFolder
from Products.CMFCore.utils import getToolByName
from Products.CMFPlone.utils import safe_unicode
from Products.statusmessages.interfaces import IStatusMessage
from z3c.form import button, field
from z3c.form.form import Form
from zope import schema
from zope.app.intid.interfaces import IIntIds
from zope.component import getUtility
from zope.interface import Interface
from zope.interface import provider
from zope.schema.interfaces import IContextAwareDefaultFactory


@provider(IContextAwareDefaultFactory)
def set_prefix_default(context):
    parent = aq_parent(aq_inner(context))

    current_prefix = IReferenceNumberPrefix(parent).get_number(context)
    if current_prefix is None:
        return None
    return int(current_prefix)


@provider(IContextAwareDefaultFactory)
def set_next_prefix_default(context):
    parent = aq_parent(aq_inner(context))

    next_prefix = IReferenceNumberPrefix(parent).get_next_number(context)
    if next_prefix is None:
        return None
    return int(next_prefix)


@provider(IContextAwareDefaultFactory)
def set_current_refnum_default(context):
    ref_number = IReferenceNumber(context).get_number()
    return safe_unicode(ref_number)


@provider(IContextAwareDefaultFactory)
def set_current_refnum_from_metadata_default(context):
    catalog = getToolByName(context, 'portal_catalog')
    brain = catalog(path='/'.join(context.getPhysicalPath()))[0]
    ref_number_from_metadata = brain.reference
    return safe_unicode(ref_number_from_metadata)


class IReferenceNumberPrefixSchema(Interface):
    prefix = schema.Int(
        title=u"Reference Number Prefix for this object",
        defaultFactory=set_prefix_default)

    next_prefix = schema.Int(
        title=u"Next available prefix",
        readonly=True,
        required=False,
        defaultFactory=set_next_prefix_default)

    current_refnum = schema.TextLine(
        title=u"Current Reference Number",
        readonly=True,
        required=False,
        defaultFactory=set_current_refnum_default)

    current_refnum_from_metadata = schema.TextLine(
        title=u"Current Reference Number from Catalog Metadata",
        readonly=True,
        required=False,
        defaultFactory=set_current_refnum_from_metadata_default)


class ReferenceNumberPrefixForm(Form):

    fields = field.Fields(IReferenceNumberPrefixSchema)

    ignoreContext = True

    @button.buttonAndHandler(u'Save')
    def save_new_prefix(self, action):
        data, errors = self.extractData()

        if len(errors) > 0:
            return

        obj = self.context
        parent = aq_parent(aq_inner(obj))

        new_prefix = unicode(data['prefix'])
        old_prefix = IReferenceNumberPrefix(parent).get_number(obj)

        intids = getUtility(IIntIds)
        intid = intids.getId(aq_base(obj))

        prefix_adapter = IReferenceNumberPrefix(parent)

        # Check if prefix already allocated
        prefix_mapping = prefix_adapter.get_prefix_mapping(obj)
        child_mapping = prefix_adapter.get_child_mapping(obj)

        if new_prefix in prefix_mapping.values() or \
        new_prefix in child_mapping.keys():
            raise Exception("This prefix is already allocated!")

        # Save new prefix in both mappings
        prefix_mapping[intid] = new_prefix
        child_mapping[new_prefix] = intid

        # Drop entry for old prefix from child_mapping
        if old_prefix in child_mapping:
            child_mapping.pop(old_prefix)

        if IRepositoryFolder.providedBy(obj):
            # Also change the prefix on the repo folder behavior
            rnp_behavior = IReferenceNumberPrefixBehavior(obj)
            rnp_behavior.reference_number_prefix = new_prefix

        obj.reindexObject()

        IStatusMessage(self.request).addStatusMessage(
            _(u"Reference Number prefix for '%s' "
                "changed to '%s'" % (obj.id, new_prefix)), 'info')

        return self.request.RESPONSE.redirect(self.context.absolute_url())
