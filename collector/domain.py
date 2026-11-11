import base64
import hashlib
from pathlib import PurePosixPath


def member_path(name):
    path = PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts or '\\' in name or not path.parts:
        raise ValueError('成员路径不安全')
    return path.as_posix()


def inspect_package(manifest, files):
    seen = set()
    records = []
    for item in manifest['members']:
        name = member_path(item['name'])
        if name in seen:
            raise ValueError('清单成员重复')
        seen.add(name)
        raw = base64.b64decode(files[name], validate=True)
        digest = hashlib.sha256(raw).hexdigest()
        if len(raw) != item['size'] or digest != item['digest']:
            raise ValueError('成员校验不符')
        key = ':'.join(str(manifest[field]) for field in ('voyage','station','generation')) + ':' + name
        records.append(dict(key=key, name=name, size=len(raw), digest=digest))
    return records


def collect(payload):
    return inspect_package(payload['manifest'], payload['files'])
