from opengever.maintenance.debughelpers import setup_app
from opengever.maintenance.debughelpers import setup_option_parser
from opengever.maintenance.debughelpers import setup_plone
from plone import api
import os
import os.path
import transaction


REPOSITORIES_FOLDER_NAME = 'opengever_repositories'


class Abort(Exception):
    pass


class RetentionPeriodFixer(object):

    def __init__(self, plone, profile):
        self.plone = plone
        self.profile = profile
        self.portal_setup = api.portal.get_tool('portal_setup')

    def run(self):
        repository_file_path = self.get_repository_file_path()

    def get_repository_file_path(self):
        profile_info = self.portal_setup.getProfileInfo(self.profile)
        profile_path = profile_info['path']

        repositories_folder = os.path.join(profile_path,
                                           REPOSITORIES_FOLDER_NAME)
        repository_filenames = [filename for filename in
                                os.listdir(repositories_folder)
                                if filename.endswith('.xlsx')]

        if len(repository_filenames) != 1:
            raise Abort("Expected one repository file but found {}, {}".format(
                        len(repository_filenames), repository_filenames))

        return os.path.join(repositories_folder, repository_filenames[0])


def main():
    app = setup_app()
    parser = setup_option_parser()
    parser.add_option("-n", dest="dry_run", action="store_true", default=False)
    parser.add_option("-p", dest="profile",
                      help="profile that contains the repository excel file.")
    options, args = parser.parse_args()

    if not options.profile:
        print "the profile (-p) argument is required."
        return
    if ":" not in options.profile:
        print "invalid profile id: '{}', missing ':'".format(options.profile)
        return

    plone = setup_plone(app, options)
    RetentionPeriodFixer(plone, options.profile).run()

    if options.dry_run:
        print 'transaction doomed because we are in dry-mode.'
        print ''
        transaction.doom()

    transaction.commit()


if __name__ == '__main__':
    main()
