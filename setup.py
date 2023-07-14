from setuptools import setup, find_packages, find_namespace_packages
from functools import partial

def version_dev(version):
    from setuptools_scm.version import get_local_node_and_date

    if version.dirty:
        return get_local_node_and_date(version)
    else:
        return ""

package_dir = {
  'dao_analyzer.cache_scripts':  'src',
}

def util_add_prefix_to_list(prefix, package_list):
    return [ '.'.join([prefix, p]) for p in package_list]

def custom_f_packages(f, *args, **kwargs):
    package_list = []
    for p, d in package_dir.items():
        package_list.append(p)
        package_list.extend(util_add_prefix_to_list(p, f(d)))
        
    return package_list

custom_find_packages = partial(custom_f_packages, find_packages)
custom_find_namespace_packages = partial(custom_f_packages, find_namespace_packages)

def main():
    setup(
        # Fix in case we need to build sdist instead
        use_scm_version={
            'local_scheme': version_dev,
            'write_to': 'src/_version.py',
        },
        setup_requires=['setuptools_scm'],
        packages = custom_find_packages(),
        namespace_packages = ['dao_analyzer'],
        package_dir = package_dir,
    )

if __name__ == "__main__":
    main()