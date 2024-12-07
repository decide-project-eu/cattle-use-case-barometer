import json
from pathlib import Path

import tableauserverclient as TSC


class Tableau:
    def __init__(
            self,
            site_url: str,
            username: str,
            password: str,
            site_name: str,
    ):
        self.__tableau_auth = TSC.TableauAuth(
            username=username,
            password=password,
            site_id=site_name
        )
        self.__tableau_server = TSC.Server(
            server_address=site_url,
            use_server_version=True
        )

    @classmethod
    def from_conf(cls, conf_path: str):
        with open(conf_path) as conf_file:
            config = json.load(conf_file)

        return cls(**config)

    def publish_hyper(self, hyper_path: str, project_name: str,
                      name: None | str = None) -> None:
        with self.__tableau_server.auth.sign_in(self.__tableau_auth):
            publish_mode = TSC.Server.PublishMode.Overwrite

            projects = TSC.Pager(self.__tableau_server.projects)
            project_id = None
            for project in projects:
                if project.name == project_name:
                    project_id = project.id
                    break
            if not project_id:
                raise ValueError(f"Project name `{project_name}` not found!")

            if not name:
                name = Path(hyper_path).stem
            datasource = TSC.DatasourceItem(
                project_id,
                name=name
            )
            datasource = self.__tableau_server.datasources.publish(
                datasource,
                hyper_path,
                publish_mode,
            )
