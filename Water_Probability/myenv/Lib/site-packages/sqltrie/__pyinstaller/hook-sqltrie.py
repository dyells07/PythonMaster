# pylint: disable=invalid-name
from PyInstaller.utils.hooks import collect_data_files  # pylint: disable=import-error

datas = collect_data_files("sqltrie")
