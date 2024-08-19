import uuid

import requests
from dvc.testing.cloud import Cloud
from dvc.testing.path_info import HTTPURLInfo


class HTTP(Cloud, HTTPURLInfo):
    @staticmethod
    def get_url(port):  # pylint: disable=arguments-differ
        dname = str(uuid.uuid4())
        return f"http://127.0.0.1:{port}/{dname}"

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

    def write_bytes(self, contents):
        assert isinstance(contents, bytes)
        response = requests.post(self.url, data=contents, timeout=300)
        assert response.status_code == 200

    @property
    def config(self):
        return {"url": self.url}

    @property
    def fs_path(self):
        return self.url

    def exists(self):
        raise NotImplementedError

    def is_dir(self):
        raise NotImplementedError

    def is_file(self):
        raise NotImplementedError

    def read_bytes(self):
        raise NotImplementedError
