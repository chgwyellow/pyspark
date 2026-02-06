from collections import namedtuple

# Define the structure for overall project settings
ProjectConfig = namedtuple(
    "ProjectConfig", ["app_name", "default_partitions", "db_name"]
)

# Global constants
APP_SETTINGS = ProjectConfig(
    app_name="Aviation_Pipeline", default_partitions="13", db_name="maintenance_db"
)
