import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from logging import getLogger
from ckanext.opendquality import blueprint
from ckanext.opendquality.cli import db


log = getLogger(__name__)


class OpendqualityPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IClick)

    # IClick
    def get_commands(self):
        return db.get_commands()

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic',
            'opendquality')
    
    # IBlueprint
    def get_blueprint(self):
        return blueprint.qa
    


