from setuptools import find_packages, setup


package_name = "looker-checker"
package_version = "0.0.1"
description = (
    "looker-checker is a command line tool to help manage your lookml codebase"
)


setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description_content_type=description,
    author="Michael Kaminsky",
    author_email="michael@kaminsky.rocks",
    url="https://github.com/mikekaminsky/looker-checker",
    packages=find_packages(),
    package_data={},
    test_suite="test",
    entry_points={"console_scripts": ["looker-checker = core.main:main"]},
    scripts=[],
    install_requires=["numpy", "lookerapi", "requests"],
)
