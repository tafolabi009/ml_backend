import importlib


def test_src_package_has_version():
    """Sanity check: importing `src` is cheap and exposes __version__.

    We avoid importing heavy submodules at top-level in __init__ so this
    test can run in CI without installing GPU or ML dependencies.
    """
    try:
        spec = importlib.util.find_spec("src")
    except Exception:
        spec = None

    # If spec is missing (tests may inject fake 'src' modules), ensure the
    # local repository path is first on sys.path so import finds the package.
    if spec is None:
        from pathlib import Path
        project_root = str(Path(__file__).resolve().parents[2])
        if project_root not in sys.path:
            sys.path.insert(0, project_root)

    import src
    assert hasattr(src, "__version__"), "src.__version__ must exist"
    assert isinstance(src.__version__, str) and src.__version__, "version must be a non-empty string"
