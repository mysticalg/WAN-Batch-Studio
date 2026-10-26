from pathlib import Path
from importlib.metadata import distribution
root=Path('dist/WANBatchStudio/third-party'); root.mkdir(parents=True,exist_ok=True)
for name in ('numpy','Pillow','websocket-client','tzdata','pyinstaller'):
    dist=distribution(name)
    for item in dist.files or []:
        if any(token in str(item).lower() for token in ('license','copying','notice')) and '.dist-info/' in str(item):
            source=Path(dist.locate_file(item))
            if source.is_file():
                target=root/name/str(item).split('.dist-info/',1)[1]
                target.parent.mkdir(parents=True,exist_ok=True); target.write_bytes(source.read_bytes())

# CPython and Tcl/Tk are bundled by PyInstaller as well.
import sys
base=Path(sys.base_prefix)
for source,relative in [(base/'LICENSE.txt','Python/LICENSE.txt'),(base/'tcl/tk8.6/license.terms','Tcl-Tk/license.terms')]:
    if not source.is_file(): raise RuntimeError('Missing runtime license: '+str(source))
    target=root/relative; target.parent.mkdir(parents=True,exist_ok=True); target.write_bytes(source.read_bytes())
