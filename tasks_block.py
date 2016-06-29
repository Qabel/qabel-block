# The following is specially crafted for qabel-block (tornado + alembic + postgresql + redis + S3 stack)

from pathlib import Path

from invoke import Collection, run
from invoke.config import DataProxy
from invoke.util import cd

from tasks_base import Project, BaseUwsgiConfiguration, get_tree_commit, commit_is_ancestor


class UwsgiConfiguration(BaseUwsgiConfiguration):
    # Options that are paths; relative paths are made absolute (relative to the working directory of this script)
    SERVER_PATH_OPTIONS = {'logging-config', 'local-storage'}

    def __init__(self, config, tree, path):
        super().__init__(config, tree, path)

        # generated stuff first, so we can override it later manually, if ever necessary
        self.sections.append(self.automagic())
        self.sections.append(self.block_config())
        self.sections.append(self.uwsgi_config())

    def automagic(self):
        """Return automatically inferred|inferrable configuration."""
        config = {
            'plugin': 'python',
            'python-worker-override': '{tree}/src/uwsgi_plumbing.py',
            'pythonpath': '{tree}/src',
            'virtualenv': '{virtualenv}',
            'chdir': '{basedir}',
            # touch-chain-reload fixed in uwsgi development branch uwsgi-2.0
            'touch-reload': '{uwsgi_ini}',
            'lazy-apps': True,
            'enable-threads': True,
        }
        return 'automatically inferred configuration', config

    def block_config(self):
        """Return block server configuration."""
        config = self.mangle_block_config(self.config)
        return 'configuration for the block server', config

    def mangle_block_config(self, block_config):
        """Prefix keys with "block-" for use in uwsgi config."""
        mangled = {}
        for key, value in block_config.items():
            key = key.replace('_', '-')
            if key == 'uwsgi':
                continue
            if key in self.SERVER_PATH_OPTIONS and value:
                value = Path(value).absolute()
            mangled['block-' + key] = value
        return DataProxy.from_data(mangled)


class Block(Project):
    def make_namespace(self):
        return Collection()

    def uwsgi_configuration(self, ctx, tree, path):
        return UwsgiConfiguration(ctx.qabel.block, tree, path)

    def migrate_db(self, ctx, config, from_tree, to_tree):
        def alembic(against, *args, hide='out'):
            alembic_path = Path(against) / 'src'
            with cd(str(alembic_path)):
                return run('alembic -x url={dsn} '.format(dsn=database) + ' '.join(args), hide=hide)

        database = ctx.qabel.block.psql_dsn

        if from_tree:
            current_commit = get_tree_commit(from_tree)
            to_commit = get_tree_commit(to_tree)

            # We are upgrading if the current current commit is an ancestor (precedes) the commit we are deploying.
            # Otherwise it's an downgrade. This is important because we need to use the correct alembic folder
            # which must hold the more recent revision. Alembic also needs to know whether it's an upgrade or
            # downgrade, it can't figure this out on it's own.
            upgrading = commit_is_ancestor(current_commit, to_commit)

            # map "abcdef (head)" to just "abcdef". Empty output means no DB state ("None")
            current_revision = alembic(from_tree, 'current').stdout.strip().split(' ')[0] or None
        else:
            upgrading = True
            current_revision = None

        # map "Rev: 129sdjsakdasd (head)" (+ extra lines) to just "129sdjsakdasd"
        to_revision = alembic(to_tree, 'history -vr +0:').stdout.split('\n')[0].split()[1]

        if current_revision == to_revision:
            print('No database migration required (alembic revision {rev})'.format(rev=current_revision))
            return
        print('Current database is at alembic revision', current_revision)
        print('Migrating to alembic revision', to_revision)
        alembic_verb = 'upgrade' if upgrading else 'downgrade'
        against = to_tree if upgrading else from_tree
        alembic(against, alembic_verb, to_revision, hide=None)
