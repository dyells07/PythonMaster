# pylint: disable=invalid-name
import subprocess  # nosec

from PyInstaller import __main__ as pyi_main  # pylint: disable=import-error


def test_pyi_hook_sqltrie(tmp_path):
    app_name = "userapp"
    workpath = tmp_path / "build"
    distpath = tmp_path / "dist"
    app = tmp_path / f"{app_name}.py"
    app.write_text("from sqltrie import SQLiteTrie; SQLiteTrie()")
    pyi_main.run(
        [
            "--workpath",
            str(workpath),
            "--distpath",
            str(distpath),
            "--specpath",
            str(tmp_path),
            str(app),
        ],
    )
    subprocess.run([str(distpath / app_name / app_name)], check=True)  # nosec
