import os


def get_hook_dirs():
    return [os.path.dirname(__file__)]


def get_PyInstaller_tests():  # pylint: disable=invalid-name
    return [os.path.dirname(__file__)]
