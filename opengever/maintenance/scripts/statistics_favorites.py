"""Favorites usage statistics.

The goal of this script is to collect usage statistics of repository folder favorites.

    bin/instance run ./scripts/statistics_favorites.py
"""

from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from opengever.ogds.base.utils import get_current_admin_unit
from opengever.portlets.tree.favorites import ANNOTATION_KEY
from opengever.repository.repositoryroot import IRepositoryRoot
from plone.i18n.normalizer import filenamenormalizer
from plone.registry.interfaces import IRegistry
from zope.annotation import IAnnotations
from zope.component import getUtility
import json
import os.path


def dump_usage_statistics(plone, directory):
    reporoots = filter(IRepositoryRoot.providedBy, plone.objectValues())
    result = {'favorites_enabled': is_favorites_feature_enabled(),
              'stats': map(stats_for_reporoot, reporoots)}
    print json.dumps(result, sort_keys=True, indent=4)
    if directory:
        dump(directory, result)


def dump(directory, result):
    filename = filenamenormalizer.normalize(get_current_admin_unit().public_url) + '.json'
    path = os.path.abspath(os.path.join(directory, filename))
    print 'Dumping to', path
    with open(path, 'w+') as fio:
        json.dump(result, fio, sort_keys=True, indent=4)


def is_favorites_feature_enabled():
    return getUtility(IRegistry).get('opengever.portlets.tree.enable_favorites')


def stats_for_reporoot(root):
    per_user = map(len, IAnnotations(root).get(ANNOTATION_KEY).values())
    positives = filter(None, per_user)
    return {'root_id': root.getId(),
            'total_users': len(per_user),
            'with_favorites': len(positives),
            'min': min(positives),
            'max': max(positives),
            'median': median(positives)}


def median(numbers):
    numbers = sorted(numbers)
    center = len(numbers) / 2
    if len(numbers) % 2 == 0:
        return sum(numbers[center - 1:center + 1]) / 2.0
    else:
        return numbers[center]


def main():
    parser = setup_option_parser()
    parser.add_option('-d', '--dump-directory', dest='directory',
                      help='Path to a directory where a JSON file is created with the output.')
    (options, args) = parser.parse_args()

    app = setup_app()
    plone = setup_plone(app, options)
    dump_usage_statistics(plone, options.directory)


if __name__ == '__main__':
    main()
