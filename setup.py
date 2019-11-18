from setuptools import setup, find_packages
import os

version = '1.0.dev0'

tests_require = [
    'ftw.bumblebee[tests]',
    'ftw.testing',
    'plone.app.testing',
    'plone.testing',
    'five.grok',
    'zope.testing',
    'opengever.core[tests]',
]

setup(name='opengever.maintenance',
      version=version,
      description="Commonly used utilities and scripts for OG maintenance.",
      long_description=open("README.rst").read() + "\n" +
                       open(os.path.join("docs", "HISTORY.txt")).read(),
      # Get more strings from
      # http://pypi.python.org/pypi?:action=list_classifiers
      classifiers=[
        "Programming Language :: Python",
        ],
      keywords='',
      author='Lukas Graf',
      author_email='lukas.graf@4teamwork.ch',
      url='http://www.4teamwork.ch',
      license='GPL',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=['opengever'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'apachelog',
          'five.z2monitor',
          'opengever.core',
          'Plone',
          'plone.api',
          'setuptools',
      ],
      tests_require=tests_require,
      extras_require=dict(tests=tests_require),
      entry_points="""
      # -*- Entry points: -*-
      [z3c.autoinclude.plugin]
      target = plone

      [console_scripts]
      analyze-logs = opengever.maintenance.analyze_logs:main

      [zopectl.command]
      get_profile_version = opengever.maintenance.commands:get_profile_version_cmd
      set_profile_version = opengever.maintenance.commands:set_profile_version_cmd
      """,
      )
