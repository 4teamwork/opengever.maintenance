from opengever.maintenance.testing import OG_MAINTENANCE_INTEGRATION
import unittest


class TestZCML(unittest.TestCase):

    layer = OG_MAINTENANCE_INTEGRATION

    def test_zcml_loading(self):
        """Load the ZCML for the opengever.maintenance package in order to
        trigger any import errors that would prevent the instance from booting.
        """

        self.layer['load_zcml_string']("""
            <configure>
                <include package="opengever.maintenance" />
            </configure>
        """)
