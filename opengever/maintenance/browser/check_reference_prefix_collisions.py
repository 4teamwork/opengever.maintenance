from Acquisition import aq_parent
from five import grok
from opengever.repository.behaviors import referenceprefix
from plone import api
from Products.CMFPlone.interfaces import IPloneSiteRoot


class CheckReferencePrefixCollisionsView(grok.View):
    """Checks whether there are any repository folders with duplicate
    reference number prefixes on the same level.
    """

    grok.name('check-reference-prefix-collisions')
    grok.context(IPloneSiteRoot)
    grok.require('cmf.ManagePortal')

    def get_prefix(self, repo_folder):
        """Get the reference number prefix for a particular repository folder
        """
        adapter = referenceprefix.IReferenceNumberPrefix(repo_folder)
        refnum_prefix = adapter.reference_number_prefix
        return refnum_prefix

    def get_siblings(self, repo_folder):
        """Get the siblings for a repository folder (other repository folders
        on the same nesting level)
        """
        parent_folder = aq_parent(repo_folder)
        siblings = []
        for folder in parent_folder.listFolderContents():
            if not folder == repo_folder:
                siblings.append(folder)
        return siblings

    def get_repo_roots(self):
        """Get all repository roots for the adapted Plone site
        """
        catalog = api.portal.get_tool(name='portal_catalog')
        repo_roots = [
            b.getObject() for b in
            catalog(portal_type='opengever.repository.repositoryroot')]
        return repo_roots

    def get_repo_folders(self, repo_root):
        """Given a repository root, return a list of all repository folders
        below that root.
        """
        catalog = api.portal.get_tool(name='portal_catalog')
        root_path = '/'.join(repo_root.getPhysicalPath())
        repo_folders = [
            b.getObject() for b in
            catalog(path=root_path,
                    portal_type='opengever.repository.repositoryfolder')]
        return repo_folders

    def find_collisions(self, repo_root):
        """Given a repository root, find all colliding reference number
        prefixes for all repository folders below that root.

        (Collision: Two repository folders on the same nesting level having
        the same reference number prefix)
        """
        collisions = []
        repo_folders = self.get_repo_folders(repo_root)

        for repo_folder in repo_folders:
            refnum_prefix = self.get_prefix(repo_folder)
            for sibling in self.get_siblings(repo_folder):
                sibling_prefix = self.get_prefix(sibling)
                if sibling_prefix == refnum_prefix:
                    collision = (repo_folder, sibling, refnum_prefix)
                    # Reverse collision - other end of the duplicate may
                    # already have been added. Only report them once
                    rev_collision = (sibling, repo_folder, refnum_prefix)
                    if rev_collision not in collisions:
                        collisions.append(collision)
        return collisions

    def format_collisions(self, collisions):
        """Format a list of collision 3-tuples as a string
        """
        output = ["Conflicting reference number prefixes:"]
        for collision in collisions:
            output.append(
                "Repositoryfolder %s and %s have the same prefix (%s)" % (
                    repr(collision[0]), repr(collision[1]), collision[2]))
        return '\n'.join(output)

    def render(self):
        collisions = []
        for root in self.get_repo_roots():
            collision_list = self.find_collisions(root)
            collisions.extend(collision_list)

        return self.format_collisions(collisions)
