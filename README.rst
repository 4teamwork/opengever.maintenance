Introduction
============

This product provides some commonly used utility functions and scripts
for OpenGever maintenance, running `bin/instance run` scripts or tasks
done in debug mode.

Get into debug mode
===================

.. code::

    $ bin/instance debug
    ...
    >>> from opengever.maintenance.debughelpers import setup_plone
    >>> setup_debug_mode()
    INFO: Using Plone Site 'mandant1'.
    >>> plone
    <PloneSite at /mandant1>
