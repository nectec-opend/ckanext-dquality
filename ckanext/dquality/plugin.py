import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from logging import getLogger
from ckanext.dquality import blueprint
from ckanext.dquality.cli import cli


log = getLogger(__name__)


class dqualityPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IClick)

    # IClick
    def get_commands(self):
        return cli.get_commands()

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('assets',
            'dquality')
    
    # IBlueprint
    def get_blueprint(self):
        return blueprint.qa


