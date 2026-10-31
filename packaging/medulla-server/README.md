# medulla-server

The native `medulla-server` executable, packaged for pip. It is built from the
[medulla](https://github.com/Knuckles-Team/medulla) source at the same
version with the size-oriented `release-server` profile.

```bash
pip install medulla-server
# or, together with the client:
pip install "medulla[server]"
```

```python
from medulla_server import binary_path

print(binary_path())  # raises BinaryNotFoundError if the executable is missing
```
