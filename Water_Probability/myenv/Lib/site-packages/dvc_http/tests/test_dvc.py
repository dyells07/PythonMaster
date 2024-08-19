import pytest
from dvc.testing.api_tests import (  # noqa, pylint: disable=unused-import
    TestAPI,
)
from dvc.testing.remote_tests import (  # noqa, pylint: disable=unused-import
    TestRemote,
)
from dvc.testing.workspace_tests import (  # noqa, pylint: disable=unused-import
    TestGetUrl,
)
from dvc.testing.workspace_tests import TestImport as _TestImport
from dvc.testing.workspace_tests import (  # noqa, pylint: disable=unused-import
    TestLsUrl,
)


@pytest.fixture
def remote(make_remote):
    yield make_remote(name="upstream", typ="http")


@pytest.fixture
def workspace(make_workspace):
    yield make_workspace(name="workspace", typ="http")


class TestImport(_TestImport):
    @pytest.fixture
    def stage_md5(self):
        return "ffe462bbb08432b7a1c3985fcf82ad3a"

    @pytest.fixture
    def is_object_storage(self):
        pytest.skip("broken")

    @pytest.fixture
    def dir_md5(self):
        pytest.skip("broken")
