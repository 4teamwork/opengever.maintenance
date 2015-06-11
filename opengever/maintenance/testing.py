from ftw.testing.layer import COMPONENT_REGISTRY_ISOLATION
from plone.app.testing import IntegrationTesting
from plone.app.testing import PloneSandboxLayer
from zope.configuration import xmlconfig


class OGMaintenanceLayer(PloneSandboxLayer):

    defaultBases = (COMPONENT_REGISTRY_ISOLATION,)

    def setUpZope(self, app, configurationContext):
        import opengever.maintenance
        xmlconfig.file('configure.zcml', opengever.maintenance,
                       context=configurationContext)

OG_MAINTENANCE_FIXTURE = OGMaintenanceLayer()

OG_MAINTENANCE_INTEGRATION = IntegrationTesting(
    bases=(OG_MAINTENANCE_FIXTURE,
           COMPONENT_REGISTRY_ISOLATION),
    name='opengever.maintenance:integration')
