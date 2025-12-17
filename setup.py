"""
Setup script for MNM Fasteners Agent.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read version from package
about = {}
exec((Path(__file__).parent / "agent" / "__init__.py").read_text(), about)

# Read README
readme = Path(__file__).parent / "README.md"
long_description = readme.read_text() if readme.exists() else ""

setup(
    name="mnm-fasteners-agent",
    version=about.get("__version__", "1.0.0"),
    author="MNM Fasteners",
    description="Agent for bridging ecommerce platforms with Sage 50",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/MNM-Fasteners-Agent",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "websockets>=12.0",
        "aiohttp>=3.9.0",
        "python-dotenv>=1.0.0",
        "pydantic>=2.0.0",
        "loguru>=0.7.0",
        "click>=8.0.0",
        "rich>=13.0.0",
        "pyjwt>=2.8.0",
        "apscheduler>=3.10.0",
        "psutil>=5.9.0",
        "orjson>=3.9.0",
    ],
    extras_require={
        "windows": [
            "pywin32>=306",
            "comtypes>=1.2.0",
        ],
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.23.0",
            "black>=23.0.0",
            "ruff>=0.1.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "mnm-agent=agent.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Win32 (MS Windows)",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Office/Business :: Financial :: Accounting",
    ],
)

