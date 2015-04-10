Introduction
============

This product provides some commonly used utility functions and scripts
for OpenGever maintenance, running ``bin/instance run`` scripts or tasks
done in debug mode.

Setting up debug mode
=====================

.. code::

    $ bin/instance debug
    ...
    >>> from opengever.maintenance import dm
    >>> dm()
    INFO: Using Plone Site 'mandant1'.
    >>> plone
    <PloneSite at /mandant1>

(``opengever.maintenance.dm`` is a convenience import that actually points to
``opengever.maintenance.debughelpers.setup_debug_mode``.)


Commands
========

The ``opengever.maintenance`` package provides some useful zopectl commands:

Get or set a profile version:
-----------------------------

.. code::

    bin/instance get_profile_version foo.bar:default
    
    bin/instance set_profile_version foo.bar:default 1

