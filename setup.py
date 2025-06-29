from setuptools import setup, find_packages
import os

# Function to read requirements.txt
def parse_requirements(filename):
    """Load requirements from a pip requirements file."""
    with open(os.path.join(os.path.dirname(__file__), 'client_py', filename), 'r') as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return lines

# Read version from a centralized place if available, e.g., client_py/version.py
# For now, hardcoding.
VERSION = "0.1.0.dev1"

# Read README for long description
with open(os.path.join(os.path.dirname(__file__), 'client_py', 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="snowflake-client-py",
    version=VERSION,
    author="Jules (AI)",
    author_email="jules@example.com", # Placeholder
    description="Python implementation of the Snowflake Pluggable Transport client.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="<repository_url>",  # Placeholder for actual repo URL
    license="BSD-3-Clause", # Match Go Snowflake license if possible, or choose appropriately
    packages=find_packages(where=".", include=['client_py', 'client_py.*']),
    # package_dir={'': '.'}, # Not needed if packages are directly under root or find_packages works as expected

    install_requires=parse_requirements('requirements.txt'),

    extras_require={
        'sqs': ['aioboto3'], # For optional SQS support
        # 'all': ['aioboto3'] # if there were more optional groups
    },

    entry_points={
        "console_scripts": [
            "snowflake-client-py=client_py.pt_launcher:main_cli_entry",
            # main_cli_entry would be a small wrapper around asyncio.run(main_pt()) in pt_launcher.py
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha", # Given KCP/SMUX stubs
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: BSD License", # Assuming compatibility
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Topic :: Internet",
        "Topic :: Security :: Cryptography",
        "Topic :: System :: Networking",
    ],
    python_requires=">=3.8", # Based on asyncio features and type hinting used

    # If your project has data files that need to be included:
    # include_package_data=True,
    # package_data={
    #     'client_py': ['some_data_file.dat'],
    # },
)
