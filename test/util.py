from sqlalchemy import __version__ as _SA_Version

SA_Version = [int(ver_token) for ver_token in _SA_Version.split(".")[0:2]]
